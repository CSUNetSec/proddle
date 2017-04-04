use bson::Bson;
use mongodb::{Client, ClientOptions, ThreadedClient};
use mongodb::db::{Database, ThreadedDatabase};
use proddle::{Operation, ProddleError};
use serde_json;

use std::collections::HashMap;

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
        let db = try!(connect(&self.inner, &self.username, &self.password, "proddle"));

        let mut measurement_failures = Vec::new();
        let mut insertion_count = 0;

        for (i, measurement) in measurements.iter().enumerate() {
            //parse string into Bson::Document
            match serde_json::from_str(measurement) {
                Ok(json) => {
                    match Bson::from_json(&json) {
                        Bson::Document(document) => {
                            //insert document
                            match db.collection("measurements").insert_one(document, None) {
                                Ok(_) => insertion_count += 1,
                                Err(e) => error!("failed to insert measurement: {}", e),
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
        
        if insertion_count != 0 {
            info!("inserted {} measurment(s)");
        }
        Ok(measurement_failures)
    }

    pub fn update_operations(&self, operation_bucket_hashes: HashMap<u64, u64>) -> Result<HashMap<u64, Vec<Operation>>, ProddleError> {
        let db = try!(connect(&self.inner, &self.username, &self.password, "proddle"));
        unimplemented!();
    }
}

fn connect(client: &Client, username: &str, password: &str, database: &str) -> Result<Database, ProddleError> {
    let db = client.db(database);
    try!(db.auth(&username, &password));
    Ok((db))
}
