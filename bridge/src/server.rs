extern crate capnp;
extern crate proddle;

use bson::{self, Bson, Document};
use capnp::capability::Promise;
use mongodb::db::{Database, ThreadedDatabase};
use proddle::{Measurement, Operation};
use proddle::proddle_capnp::proddle::{GetMeasurementsParams, GetMeasurementsResults, GetOperationsParams, GetOperationsResults, SendResultsParams, SendResultsResults};
use proddle::proddle_capnp::proddle::Server;
use serde_json;

use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::{DefaultHasher, Entry};
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
    mongodb_db: Database,
}

impl ServerImpl {
    pub fn new(mongodb_db: Database) -> ServerImpl {
        ServerImpl {
            mongodb_db: mongodb_db,
        }
    }
}

impl Server for ServerImpl {
    fn get_measurements(&mut self, params: GetMeasurementsParams<>, mut results: GetMeasurementsResults<>) -> Promise<(), capnp::Error> {
        let param_measurements = pry!(pry!(params.get()).get_measurements());
        
        //iterate over measurements in mongodb and store in measurements map
        let mut measurements = HashMap::new();
        let cursor = cry!(proddle::find_measurements(&self.mongodb_db, None, None, None, false));
        for document in cursor {
            let document = cry!(document);

            //parse mongodb document into measurement
            let measurement: Measurement = cry!(bson::from_bson(Bson::Document(document)));

            //check if measurement name already exists and/or version comparrison
            let entry = measurements.entry(measurement.name.to_owned());
            if let Entry::Occupied(mut occupied_entry) = entry {
                let replace;
                {
                    let entry: &Measurement = occupied_entry.get();
                    replace = measurement.version > entry.version;
                }
                
                //if version is newer then replace
                if replace {
                    occupied_entry.insert(measurement);
                }
            } else if let Entry::Vacant(vacant_entry) = entry {
                //if entry does not exist then insert
                vacant_entry.insert(measurement);
            }
        }

        //compare vantage measurement versions to mongodb versions
        for measurement in param_measurements.iter() {
            let measurement_name = cry!(measurement.get_name());

            let entry = measurements.entry(measurement_name.to_owned());
            if let Entry::Occupied(occupied_entry) = entry {
                //if vantage version is up to date remove from mongodb map
                if measurement.get_version() >= occupied_entry.get().version {
                    occupied_entry.remove();
                }
            } else if let Entry::Vacant(vacant_entry) = entry {
                //if mongodb map doens't contain remove from vantage
                vacant_entry.insert(Measurement::new(None, measurement_name.to_owned(), 0, None, None, None));
            }
        }

        //create results message
        let mut results_measurements = results.get().init_measurements(measurements.len() as u32);
        for (i, entry) in measurements.values().enumerate() {
            //populate measurement
            let mut measurement = results_measurements.borrow().get(i as u32);

            if let Some(timestamp) = entry.timestamp {
                measurement.set_timestamp(timestamp);
            }

            measurement.set_name(&entry.name);
            measurement.set_version(entry.version);

            if let Some(ref parameters) = entry.parameters {
                let mut measurement_parameters = measurement.borrow().init_parameters(parameters.len() as u32);
                for (i, parameter) in parameters.iter().enumerate() {
                    let mut measurement_parameter = measurement_parameters.borrow().get(i as u32);
                    measurement_parameter.set_name(&parameter.name);
                    measurement_parameter.set_value(&parameter.value);
                }
            }

            if let Some(ref dependencies) = entry.dependencies {
                let mut measurement_dependencies = measurement.borrow().init_dependencies(dependencies.len() as u32);
                for (i, dependency) in dependencies.iter().enumerate() {
                    measurement_dependencies.set(i as u32, dependency);
                }
            }

            if let Some(ref content) = entry.content {
                measurement.set_content(&content);
            }
        }

        if measurements.len() != 0 {
            info!("updated {} measurement(s) in reply", measurements.len());
        }
        Promise::ok(())
    }

    fn get_operations(&mut self, params: GetOperationsParams<>, mut results: GetOperationsResults<>) -> Promise<(), capnp::Error> {
        let param_bucket_hashes = pry!(pry!(params.get()).get_bucket_hashes());

        //initialize bridge side bucket hashes
        let mut bucket_hashes = BTreeMap::new();
        let mut operations: BTreeMap<u64, Vec<Operation>> = BTreeMap::new();
        for bucket_hash in param_bucket_hashes.iter() {
            bucket_hashes.insert(bucket_hash.get_bucket(), DefaultHasher::new());
            operations.insert(bucket_hash.get_bucket(), Vec::new());
        }
        
        //iterate over operations in mongodb and store in operations map
        let cursor = cry!(proddle::find_operations(&self.mongodb_db, None, None, None, false));
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

                result_operation.set_measurement(&operation.measurement);
                result_operation.set_domain(&operation.domain);
                result_operation.set_url(&operation.url);

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

    fn send_results(&mut self, params: SendResultsParams<>, _: SendResultsResults<>) -> Promise<(), capnp::Error> {
        let param_results = pry!(pry!(params.get()).get_results());

        //iterate over results
        let mut count = 0;
        for param_result in param_results.iter() {
            let json_string = param_result.get_json_string().unwrap();

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
            if let Err(e) = self.mongodb_db.collection("results").insert_one(document, None) {
                error!("{}", e);
            }

            count += 1;
        }

        info!("handled {} results", count);
        Promise::ok(())
    }
}
