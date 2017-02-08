#[macro_use(bson, doc)]
extern crate bson;
extern crate capnp;
extern crate clap;
extern crate mongodb;

mod error;
mod measurement;
mod operation;

pub mod proddle_capnp {
    include!(concat!(env!("OUT_DIR"), "/proddle_capnp.rs"));
}

pub use self::error::Error;
pub use self::measurement::Measurement;
pub use self::operation::Operation;

use bson::ordered::OrderedDocument;
use mongodb::{Client, ThreadedClient};
use mongodb::coll::options::{CursorType, FindOptions};
use mongodb::cursor::Cursor;
use mongodb::db::{Database, ThreadedDatabase};

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/*
 * Miscellaneous
 */
pub fn hash_string(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub fn get_bucket_key(map: &BTreeMap<u64, DefaultHasher>, key: u64) -> Option<u64> {
    let mut bucket_key = 0;
    for map_key in map.keys() {
        if *map_key > key {
            break;
        }

        bucket_key = *map_key;
    }

    Some(bucket_key)
}

/*
 * MongoDB
 */
pub fn get_mongodb_client(host: &str, port: u16) -> Result<Client, Error> {
    Ok(try!(Client::connect(host, port)))
}

pub fn find_measurement(db: &Database, measurement_name: &str, version: Option<i32>, order_by_version: bool) -> Result<Option<OrderedDocument>, Error> {
    //create search document
    let search_document = match version {
        Some(search_version) => Some(doc! { "name" => measurement_name, "version" => search_version }),
        None => Some(doc! { "name" => measurement_name }),
    };

    //create find options
    let negative_one = -1;
    let sort_document = match order_by_version {
        true => Some(doc! { "version" => negative_one }),
        false => None,
    };

    let find_options = Some(FindOptions {
        allow_partial_results: false,
        no_cursor_timeout: false,
        op_log_replay: false,
        skip: 0,
        limit: 1,
        cursor_type: CursorType::NonTailable,
        batch_size: 0,
        comment: None,
        max_time_ms: None,
        modifiers: None,
        projection: None,
        sort: sort_document,
        read_preference: None,
    });

    //execute find one
    Ok(try!(db.collection("measurements").find_one(search_document, find_options)))
}

pub fn find_measurements(db: &Database, measurement_name: Option<&str>, version: Option<i32>, limit: Option<i32>, order_by_version: bool) -> Result<Cursor, Error> {
    //create search document
    let search_document = match measurement_name {
        Some(search_measurement_name) => {
            match version {
                Some(search_version) => Some(doc! { "name" => search_measurement_name, "version" => search_version }),
                None => Some(doc! { "name" => search_measurement_name }),
            }
        },
        None => {
            match version {
                Some(search_version) => Some(doc! { "version" => search_version }),
                None => None,
            }
        },
    };

    //create find options
    let negative_one = -1;
    let sort_document = match order_by_version {
        true => Some(doc! { "version" => negative_one }),
        false => None,
    };

    let find_options = Some(FindOptions {
        allow_partial_results: false,
        no_cursor_timeout: false,
        op_log_replay: false,
        skip: 0,
        limit: limit.unwrap_or(0),
        cursor_type: CursorType::NonTailable,
        batch_size: 0,
        comment: None,
        max_time_ms: None,
        modifiers: None,
        projection: None,
        sort: sort_document,
        read_preference: None,
    });

    //execute find
    Ok(try!(db.collection("measurements").find(search_document, find_options)))
}

pub fn find_operation(db: &Database, domain: &str, measurement_name: Option<&str>, order_by_timestamp: bool) -> Result<Option<OrderedDocument>, Error> {
    //create search document
    let search_document = match measurement_name {
        Some(search_measurement_name) => Some(doc! { "domain" => domain, "measurement" => search_measurement_name }),
        None => Some(doc! { "domain" => domain }),
    };

    //create find options
    let negative_one = -1;
    let sort_document = match order_by_timestamp {
        true => Some(doc! { "timestamp" => negative_one }),
        false => Some(doc! { "_id" => 1 }),
    };

    let find_options = Some(FindOptions {
        allow_partial_results: false,
        no_cursor_timeout: false,
        op_log_replay: false,
        skip: 0,
        limit: 1,
        cursor_type: CursorType::NonTailable,
        batch_size: 0,
        comment: None,
        max_time_ms: None,
        modifiers: None,
        projection: None,
        sort: sort_document,
        read_preference: None,
    });

    //execute find one
    Ok(try!(db.collection("operations").find_one(search_document, find_options)))
}

pub fn find_operations(db: &Database, domain: Option<&str>, measurement_name: Option<&str>, limit: Option<i32>, order_by_timestamp: bool) -> Result<Cursor, Error> {
    //create search document
    let search_document = match domain {
        Some(domain) => {
            match measurement_name {
                Some(search_measurement_name) => Some(doc! { "domain" => domain, "measurement" => search_measurement_name }),
                None => Some(doc! { "domain" => domain }),
            }
        },
        None => {
            match measurement_name {
                Some(search_measurement_name) => Some(doc! { "measurement" => search_measurement_name }),
                None => None,
            }
        }
    };

    //create find options
    let negative_one = -1;
    let sort_document = match order_by_timestamp {
        true => Some(doc! { "timestamp" => negative_one }),
        false => Some(doc! { "_id" => 1 }),
    };

    let find_options = Some(FindOptions {
        allow_partial_results: false,
        no_cursor_timeout: false,
        op_log_replay: false,
        skip: 0,
        limit: limit.unwrap_or(0),
        cursor_type: CursorType::NonTailable,
        batch_size: 0,
        comment: None,
        max_time_ms: None,
        modifiers: None,
        projection: None,
        sort: sort_document,
        read_preference: None,
    });

    //specify operations collection
    Ok(try!(db.collection("operations").find(search_document, find_options)))
}
