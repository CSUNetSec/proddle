use mongodb::{Client, ClientOptions, ThreadedClient};
use proddle::{Operation, ProddleError};

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

    pub fn send_measurements(&self, measurements: Vec<String>) -> Result<Vec<bool>, ProddleError> {
        unimplemented!();
    }

    pub fn update_operations(&self, operation_bucket_hashes: HashMap<u64, u64>) -> Result<HashMap<u64, Vec<Operation>>, ProddleError> {
        unimplemented!();
    }
}
