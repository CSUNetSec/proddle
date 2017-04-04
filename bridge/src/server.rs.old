extern crate capnp;
extern crate proddle;

use bson::{self, Bson, Document};
use capnp::capability::Promise;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use proddle::Operation;
use proddle::proddle_capnp::proddle::{GetOperationsParams, GetOperationsResults, SendMeasurementsParams, SendMeasurementsResults};
use proddle::proddle_capnp::proddle::Server;
use serde_json;

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

macro_rules! cry {
    ($e:expr) => (match $e {
        Ok(val) => val,
        Err(e) => {
            error!("{}", e);
            return Promise::err(capnp::Error::failed(String::from("internal error")));
        }
    });
}

pub struct ServerImpl {
    mongodb_client: Client,
    username: String,
    password: String,
}

impl ServerImpl {
    pub fn new(mongodb_client: Client, username: String, password: String) -> ServerImpl {
        ServerImpl {
            mongodb_client: mongodb_client,
            username: username,
            password: password,
        }
    }
}

impl Server for ServerImpl {
    fn get_operations(&mut self, params: GetOperationsParams<>, mut results: GetOperationsResults<>) -> Promise<(), capnp::Error> {
        let proddle_db = self.mongodb_client.db("proddle");
        cry!(proddle_db.auth(&self.username, &self.password));

        let param_bucket_hashes = pry!(pry!(params.get()).get_bucket_hashes());

        //initialize bridge side bucket hashes
        let mut bucket_hashes = BTreeMap::new();
        let mut operations: BTreeMap<u64, Vec<Operation>> = BTreeMap::new();
        for bucket_hash in param_bucket_hashes.iter() {
            bucket_hashes.insert(bucket_hash.get_bucket(), DefaultHasher::new());
            operations.insert(bucket_hash.get_bucket(), Vec::new());
        }
        
        //iterate over operations in mongodb and store in operations map
        let cursor = cry!(proddle::find_operations(&proddle_db, None, None, None, false));
        for document in cursor {
            let document = cry!(document);

            //parse mongodb document into measurement
            let operation: Operation = cry!(bson::from_bson(Bson::Document(document)));

            //hash domain to determine bucket key
            let domain_hash = proddle::hash_string(&operation.domain);
            let bucket_key = cry!(proddle::get_bucket_key(&bucket_hashes, domain_hash).ok_or("failed to retrieve bucket_key"));

            //add operation to bucket hashes and operations maps
            let mut hasher = cry!(bucket_hashes.get_mut(&bucket_key).ok_or("failed to retrieve hasher"));
            operation.hash(hasher);

            let mut vec = cry!(operations.get_mut(&bucket_key).ok_or("failed to retrieve bucket"));
            vec.push(operation);
        }

        //compare vantage hashes to bridge hashes
        for param_bucket_hash in param_bucket_hashes.iter() {
            let bucket_key = param_bucket_hash.get_bucket();

            //if vantage hash equals bridge hash remove vector of operations from operations
            let bucket_hash = bucket_hashes.get(&bucket_key).unwrap().finish();
            if bucket_hash == param_bucket_hash.get_hash() {
                operations.remove(&bucket_key);
            }
        }

        //create results message
        let mut result_operation_buckets = results.get().init_operation_buckets(operations.len() as u32);
        for (i, (bucket_key, operation_vec)) in operations.iter().enumerate() {
            //populate operation bucket
            let mut result_operation_bucket = result_operation_buckets.borrow().get(i as u32);
            result_operation_bucket.set_bucket(*bucket_key);

            let mut result_operations = result_operation_bucket.init_operations(operation_vec.len() as u32);
            for (j, operation) in operation_vec.iter().enumerate() {
                //populate operations
                let mut result_operation = result_operations.borrow().get(j as u32);

                if let Some(timestamp) = operation.timestamp {
                    result_operation.set_timestamp(timestamp);
                }

                result_operation.set_measurement_class(&operation.measurement_class);
                result_operation.set_domain(&operation.domain);

                if let Some(ref parameters) = operation.parameters {
                    let mut operation_parameters = result_operation.borrow().init_parameters(parameters.len() as u32);
                    for (i, parameter) in parameters.iter().enumerate() {
                        let mut operation_parameter = operation_parameters.borrow().get(i as u32);
                        operation_parameter.set_name(&parameter.name);
                        operation_parameter.set_value(&parameter.value);
                    }
                }

                if let Some(ref tags) = operation.tags {
                    let mut operation_tags = result_operation.borrow().init_tags(tags.len() as u32);
                    for (i, tag) in tags.iter().enumerate() {
                        operation_tags.set(i as u32, tag);
                    }
                }
            }
        }

        if operations.len() != 0 {
            info!("updated {} operation bucket(s) in reply", operations.len());
        }
        Promise::ok(())
    }

    fn send_measurements(&mut self, params: SendMeasurementsParams<>, _: SendMeasurementsResults<>) -> Promise<(), capnp::Error> {
        let proddle_db = self.mongodb_client.db("proddle");
        cry!(proddle_db.auth(&self.username, &self.password));

        let param_measurements = pry!(pry!(params.get()).get_measurements());

        //iterate over results
        let mut count = 0;
        for param_measurement in param_measurements.iter() {
            let json_string = param_measurement.get_json_string().unwrap();

            //parse json string into Bson::Document
            let json = match serde_json::from_str(json_string) {
                Ok(json) => json,
                Err(e) => {
                    error!("failed to parse json string '{}' :{}", json_string, e);
                    continue;
                },
            };

            let document: Document = match Bson::from_json(&json) {
                Bson::Document(document) => document,
                _ => cry!(Err(format!("failed to parse json as Bson::Document, {}", json_string))),
            };

            //insert document
            if let Err(e) = proddle_db.collection("measurements").insert_one(document, None) {
                error!("{}", e);
            }

            count += 1;
        }

        info!("handled {} measurement(s)", count);
        Promise::ok(())
    }
}

