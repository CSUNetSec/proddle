use bson::{self, Bson};
use mongodb::{Client, ClientOptions, ThreadedClient};
use mongodb::db::{Database, ThreadedDatabase};
use proddle::{Operation, ProddleError};
use serde_json;

use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct DbWrapper {
    inner: Client,
    username: String,
    password: String,
}

impl DbWrapper {
    pub fn new(ip_address: &str, port: u16, username: &str, password: &str, ca_file: &str, 
               certificate_file: &str, key_file: &str) -> Result<DbWrapper, ProddleError> {
        let client = if ca_file.eq("") && certificate_file.eq("") && key_file.eq("") {
            try!(Client::connect(&ip_address, port))
        } else {
            let client_options = ClientOptions::with_ssl(&ca_file, &certificate_file, &key_file, true);
            try!(Client::connect_with_options(&ip_address, port, client_options))
        };

        Ok(
            DbWrapper {
                inner: client,
                username: username.to_owned(),
                password: password.to_owned(),
            }
        )
    }

    pub fn send_measurements(&self, measurements: Vec<String>) -> Result<Vec<usize>, ProddleError> {
        //connect to db
        let db = try!(connect(&self.inner, &self.username, &self.password, "proddle"));

        let mut measurement_failures = Vec::new();
        for (i, measurement) in measurements.iter().enumerate() {
            //parse string into Bson::Document
            match serde_json::from_str(measurement) {
                Ok(json) => {
                    match Bson::from_json(&json) {
                        Bson::Document(document) => {
                            //insert document
                            if let Err(e) = db.collection("measurements").insert_one(document, None) {
                                error!("failed to insert measurement: {}", e);
                            }
                        },
                        _ => {
                            error!("failed to parse json as Bson::Document");
                            measurement_failures.push(i);
                        },
                    }
                },
                Err(e) => {
                    error!("failed to parse measurement '{}': {}", measurement, e);
                    measurement_failures.push(i);
                },
            }
        }
        
        Ok(measurement_failures)
    }

    pub fn update_operations(&self, operation_bucket_hashes: HashMap<u64, u64>) -> Result<HashMap<u64, Vec<Operation>>, ProddleError> {
        //connect to db
        let db = try!(connect(&self.inner, &self.username, &self.password, "proddle"));
 
        //initialize bridge side bucket hashes
        let mut s_operation_bucket_hashes = BTreeMap::new();
        let mut s_operations: HashMap<u64, Vec<Operation>> = HashMap::new();
        for bucket_key in operation_bucket_hashes.keys() {
            s_operation_bucket_hashes.insert(*bucket_key, DefaultHasher::new());
            s_operations.insert(*bucket_key, Vec::new());
        }

        //cycle through operations on db
        let cursor = try!(db.collection("operations").find(None, None));
        for document in cursor {
            let document = try!(document);

            //parse mongodb document into measurement
            let operation: Operation = try!(bson::from_bson(Bson::Document(document)));

            //hash domain to determine bucket key
            let domain_hash = hash_string(&operation.domain);
            let bucket_key = try!(get_bucket_key(&s_operation_bucket_hashes, domain_hash).ok_or("failed to retrieve bucket_key"));

            //add operation to bucket hashes and operations maps
            let mut hasher = try!(s_operation_bucket_hashes.get_mut(&bucket_key).ok_or("failed to retrieve hasher"));
            operation.hash(hasher);

            let mut vec = try!(s_operations.get_mut(&bucket_key).ok_or("failed to retrieve bucket"));
            vec.push(operation);
        }

        //compare vantage hashes to bridge hashes
        for (key, value) in operation_bucket_hashes.iter() {
            //if vantage hash equals bridge hash remove vector of operations from operations
            let s_operation_bucket_hash = s_operation_bucket_hashes.get(&key).unwrap().finish();
            if s_operation_bucket_hash == *value {
                s_operations.remove(&key);
            }
        }

        Ok(s_operations)
    }
}

fn connect(client: &Client, username: &str, password: &str, database: &str) -> Result<Database, ProddleError> {
    let db = client.db(database);
    try!(db.auth(&username, &password));
    Ok((db))
}

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
