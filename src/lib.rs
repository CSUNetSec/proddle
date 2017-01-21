#[macro_use(bson, doc)]
extern crate bson;
extern crate capnp;
extern crate mongodb;

pub mod error;
pub mod proddle_capnp {
    include!(concat!(env!("OUT_DIR"), "/proddle_capnp.rs"));
}

use error::Error;

use bson::Bson;
use bson::ordered::OrderedDocument;
use mongodb::{Client, ClientInner, ThreadedClient};
use mongodb::coll::options::{CursorType, FindOptions};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
 * Measurement
 */
pub struct Measurement {
    pub timestamp: Option<u64>,
    pub name: String,
    pub version: u16,
    pub dependencies: Option<Vec<String>>,
    pub content: Option<String>,
}

impl Measurement {
    pub fn new(timestamp: Option<u64>, name: String, version: u16, dependencies: Option<Vec<String>>, content: Option<String>) -> Measurement {
        Measurement {
            timestamp: timestamp,
            name: name,
            version: version,
            dependencies: dependencies,
            content: content,
        }
    }

    pub fn from_mongodb(document: &OrderedDocument) -> Result<Measurement, Error> {
        let timestamp = match document.get("timestamp") {
            Some(&Bson::I64(timestamp)) => Some(timestamp as u64),
            _ => return Err(Error::Proddle("failed to parse timestamp as i64".to_owned())),
        };

        let measurement_name = match document.get("name") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err(Error::Proddle("failed to parse name as string".to_owned())),
        };

        let version = match document.get("version") {
            Some(&Bson::I32(version)) => version as u16,
            _ => return Err(Error::Proddle("failed to parse version as i32".to_owned())),
        };

        let dependencies: Option<Vec<String>> = match document.get("dependencies") {
            Some(&Bson::Array(ref dependencies)) => Some(dependencies.iter().map(|x| x.to_string()).collect()),
            _ => return Err(Error::Proddle("failed to parse dependencies as array".to_owned())),
        };
        
        let content = match document.get("content") {
            Some(&Bson::String(ref content)) => Some(content.to_owned()),
            _ => return Err(Error::Proddle("failed to parse content as string".to_owned())),
        };

        Ok(
            Measurement {
                timestamp: timestamp,
                name: measurement_name,
                version: version,
                dependencies: dependencies,
                content: content,
            }
        )
    }

    pub fn from_capnproto(msg: &proddle_capnp::measurement::Reader) -> Result<Measurement, Error> {
        let timestamp = match msg.get_timestamp() {
            0 => None,
            _ => Some(msg.get_timestamp()),
        };

        let measurement_name = msg.get_name().unwrap().to_owned();
        let version = msg.get_version();

        let dependencies = match msg.has_dependencies() {
            true => Some(msg.get_dependencies().unwrap().iter().map(|x| x.unwrap().to_string()).collect()),
            false => None,
        };

        let content = match msg.has_content() {
            true => Some(msg.get_content().unwrap().to_owned()),
            false => None,
        };

        Ok(
            Measurement {
                timestamp: timestamp,
                name: measurement_name,
                version: version,
                dependencies: dependencies,
                content: content,
            }
        )
    }
}

/*
 * Operation
 */

#[derive(Clone)]
pub struct Operation {
    pub timestamp: Option<u64>,
    pub domain: String,
    pub measurement: String,
    pub interval: u32,
}

impl Operation {
    pub fn from_mongodb(document: &OrderedDocument) -> Result<Operation, Error> {
        let timestamp = match document.get("timestamp") {
            Some(&Bson::I64(timestamp)) => Some(timestamp as u64),
            _ => return Err(Error::Proddle("failed to parse timestamp as i64".to_owned())),
        };

        let domain = match document.get("domain") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err(Error::Proddle("failed to domain as string".to_owned())),
        };

        let measurement = match document.get("measurement") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err(Error::Proddle("failed to parse measurement name as string".to_owned())),
        };

        let interval = match document.get("interval") {
            Some(&Bson::I32(interval)) => interval as u32,
            _ => return Err(Error::Proddle("failed to parse interval as i32".to_owned())),
        };

        Ok(
            Operation {
                timestamp: timestamp,
                domain: domain,
                measurement: measurement,
                interval: interval,
            }
        )
    }

    pub fn from_capnproto(msg: &proddle_capnp::operation::Reader) -> Result<Operation, Error> {
        let timestamp = match msg.get_timestamp() {
            0 => None,
            _ => Some(msg.get_timestamp()),
        };

        let domain = msg.get_domain().unwrap().to_owned();
        let measurement = msg.get_measurement().unwrap().to_owned();
        let interval = msg.get_interval();

        Ok(
            Operation {
                timestamp: timestamp,
                domain: domain,
                measurement: measurement,
                interval: interval,
            }
        )
    }
}

impl Hash for Operation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.domain.hash(state);
        self.measurement.hash(state);
    }
}

/*
 * MongoDB
 */
pub fn get_mongodb_client(host: &str, port: u16) -> Result<Client, Error> {
    Ok(try!(Client::connect(host, port)))
}

pub fn find_measurement(client: Arc<ClientInner>, measurement_name: &str, version: Option<i32>, order_by_version: bool) -> Result<Option<OrderedDocument>, Error> {
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
    Ok(try!(client.db("proddle").collection("measurements").find_one(search_document, find_options)))
}

pub fn find_measurements(client: Arc<ClientInner>, measurement_name: Option<&str>, version: Option<i32>, limit: Option<i32>, order_by_version: bool) -> Result<Cursor, Error> {
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
    Ok(try!(client.db("proddle").collection("measurements").find(search_document, find_options)))
}

pub fn find_operation(client: Arc<ClientInner>, domain: &str, measurement_name: Option<&str>, order_by_timestamp: bool) -> Result<Option<OrderedDocument>, Error> {
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
    Ok(try!(client.db("proddle").collection("operations").find_one(search_document, find_options)))
}

pub fn find_operations(client: Arc<ClientInner>, domain: Option<&str>, measurement_name: Option<&str>, limit: Option<i32>, order_by_timestamp: bool) -> Result<Cursor, Error> {
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
    Ok(try!(client.db("proddle").collection("operations").find(search_document, find_options)))
}
