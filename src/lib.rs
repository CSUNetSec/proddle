#[macro_use(bson, doc)]
extern crate bson;
extern crate capnp;
extern crate mongodb;

pub mod proddle_capnp {
    include!(concat!(env!("OUT_DIR"), "/proddle_capnp.rs"));
}

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
 * HASHING
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
 *
 */
pub struct Module {
    pub timestamp: Option<u64>,
    pub name: String,
    pub version: u16,
    pub dependencies: Option<Vec<String>>,
    pub content: Option<String>,
}

impl Module {
    pub fn new(timestamp: Option<u64>, name: String, version: u16, dependencies: Option<Vec<String>>, content: Option<String>) -> Module {
        Module {
            timestamp: timestamp,
            name: name,
            version: version,
            dependencies: dependencies,
            content: content,
        }
    }

    pub fn from_mongodb(document: &OrderedDocument) -> Result<Module, String> {
        let timestamp = match document.get("timestamp") {
            Some(&Bson::I64(timestamp)) => Some(timestamp as u64),
            _ => return Err("failed to parse timestamp as i64".to_owned()),
        };

        let module_name = match document.get("name") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err("failed to parse name as string".to_owned()),
        };

        let version = match document.get("version") {
            Some(&Bson::I32(version)) => version as u16,
            _ => return Err("failed to parse version as i32".to_owned()),
        };

        let dependencies: Option<Vec<String>> = match document.get("dependencies") {
            Some(&Bson::Array(ref dependencies)) => Some(dependencies.iter().map(|x| x.to_string()).collect()),
            _ => return Err("failed to parse dependencies as array".to_owned()),
        };
        
        let content = match document.get("content") {
            Some(&Bson::String(ref content)) => Some(content.to_owned()),
            _ => return Err("failed to parse content as string".to_owned()),
        };

        Ok(
            Module {
                timestamp: timestamp,
                name: module_name,
                version: version,
                dependencies: dependencies,
                content: content,
            }
        )
    }

    pub fn from_capnproto(msg: &proddle_capnp::module::Reader) -> Result<Module, String> {
        let timestamp = match msg.get_timestamp() {
            0 => None,
            _ => Some(msg.get_timestamp()),
        };

        let module_name = msg.get_name().unwrap().to_owned();
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
            Module {
                timestamp: timestamp,
                name: module_name,
                version: version,
                dependencies: dependencies,
                content: content,
            }
        )
    }
}

#[derive(Clone)]
pub struct Operation {
    pub timestamp: Option<u64>,
    pub domain: String,
    pub module: String,
    pub interval: u32,
}

impl Operation {
    pub fn from_mongodb(document: &OrderedDocument) -> Result<Operation, String> {
        let timestamp = match document.get("timestamp") {
            Some(&Bson::I64(timestamp)) => Some(timestamp as u64),
            _ => return Err("failed to parse timestamp as i64".to_owned()),
        };

        let domain = match document.get("domain") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err("failed to domain as string".to_owned()),
        };

        let module = match document.get("module") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err("failed to parse module name as string".to_owned()),
        };

        let interval = match document.get("interval") {
            Some(&Bson::I32(interval)) => interval as u32,
            _ => return Err("failed to parse interval as i32".to_owned()),
        };

        Ok(
            Operation {
                timestamp: timestamp,
                domain: domain,
                module: module,
                interval: interval,
            }
        )
    }

    pub fn from_capnproto(msg: &proddle_capnp::operation::Reader) -> Result<Operation, String> {
        let timestamp = match msg.get_timestamp() {
            0 => None,
            _ => Some(msg.get_timestamp()),
        };

        let domain = msg.get_domain().unwrap().to_owned();
        let module = msg.get_module().unwrap().to_owned();
        let interval = msg.get_interval();

        Ok(
            Operation {
                timestamp: timestamp,
                domain: domain,
                module: module,
                interval: interval,
            }
        )
    }
}

impl Hash for Operation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.domain.hash(state);
        self.module.hash(state);
    }
}

/*
 * MONGODB
 */
pub fn get_mongodb_client(host: &str, port: u16) -> Result<Client, mongodb::Error> {
    Client::connect(host, port)
}

pub fn find_module(client: Arc<ClientInner>, module_name: &str, version: Option<i32>, order_by_version: bool) -> Result<Option<OrderedDocument>, mongodb::Error> {
    //create search document
    let search_document = match version {
        Some(search_version) => Some(doc! { "name" => module_name, "version" => search_version }),
        None => Some(doc! { "name" => module_name }),
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
    client.db("proddle").collection("modules").find_one(search_document, find_options)
}

pub fn find_modules(client: Arc<ClientInner>, module_name: Option<&str>, version: Option<i32>, limit: Option<i32>, order_by_version: bool) -> Result<Cursor, mongodb::Error> {
    //create search document
    let search_document = match module_name {
        Some(search_module_name) => {
            match version {
                Some(search_version) => Some(doc! { "name" => search_module_name, "version" => search_version }),
                None => Some(doc! { "name" => search_module_name }),
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
    client.db("proddle").collection("modules").find(search_document, find_options)
}

pub fn find_operation(client: Arc<ClientInner>, domain: &str, module_name: Option<&str>, order_by_timestamp: bool) -> Result<Option<OrderedDocument>, mongodb::Error> {
    //create search document
    let search_document = match module_name {
        Some(search_module_name) => Some(doc! { "domain" => domain, "module" => search_module_name }),
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
    client.db("proddle").collection("operations").find_one(search_document, find_options)
}

pub fn find_operations(client: Arc<ClientInner>, domain: Option<&str>, module_name: Option<&str>, limit: Option<i32>, order_by_timestamp: bool) -> Result<Cursor, mongodb::Error> {
    //create search document
    let search_document = match domain {
        Some(domain) => {
            match module_name {
                Some(search_module_name) => Some(doc! { "domain" => domain, "module" => search_module_name }),
                None => Some(doc! { "domain" => domain }),
            }
        },
        None => {
            match module_name {
                Some(search_module_name) => Some(doc! { "module" => search_module_name }),
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
    client.db("proddle").collection("operations").find(search_document, find_options)
}
