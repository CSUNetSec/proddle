use mongodb::{Client, ClientOptions, ThreadedClient};
use proddle::ProddleError;

pub struct MongodbClient {
    inner: Client,
    username: String,
    password: String,
}

impl MongodbClient {
    pub fn new(ip_address: &str, port: u16, username: &str, password: &str, ca_file: &str, 
               certificate_file: &str, key_file: &str) -> Result<MongodbClient, ProddleError> {
        let client = if ca_file.eq("") && certificate_file.eq("") && key_file.eq("") {
            try!(Client::connect(&ip_address, port))
        } else {
            let client_options = ClientOptions::with_ssl(&ca_file, &certificate_file, &key_file, true);
            try!(Client::connect_with_options(&ip_address, port, client_options))
        };

        Ok(
            MongodbClient {
                inner: client,
                username: username.to_owned(),
                password: password.to_owned(),
            }
        )
    }
}
