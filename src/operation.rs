extern crate bson;

use bson::Bson;
use bson::ordered::OrderedDocument;

use error::Error;
use proddle_capnp;

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct Operation {
    pub timestamp: Option<u64>,
    pub measurement: String,
    pub domain: String,
    pub url: String,
    pub parameters: Option<HashMap<String, String>>,
    pub interval: u32,
    pub tags: Option<Vec<String>>,
}

impl Operation {
    pub fn from_mongodb(document: &OrderedDocument) -> Result<Operation, Error> {
        let timestamp = match document.get("timestamp") {
            Some(&Bson::I64(timestamp)) => Some(timestamp as u64),
            _ => return Err(Error::Proddle(String::from("failed to parse timestamp as i64"))),
        };

        let measurement = match document.get("measurement") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err(Error::Proddle(String::from("failed to parse measurement name as string"))),
        };

        let domain = match document.get("domain") {
            Some(&Bson::String(ref domain)) => domain.to_owned(),
            _ => return Err(Error::Proddle(String::from("failed to domain as string"))),
        };

        let url = match document.get("url") {
            Some(&Bson::String(ref url)) => url.to_owned(),
            _ => return Err(Error::Proddle(String::from("failed to url as string"))),
        };

        let parameters: Option<HashMap<String, String>> = match document.get("parameters") {
            Some(&Bson::Array(ref parameters)) => {
                let mut hash_map = HashMap::new();
                for parameter in parameters.iter() {
                    let document = match parameter {
                        &Bson::Document(ref document) => document,
                        _ => return Err(Error::Proddle(String::from("failed to parameter name as bson document"))),
                    };

                    let name = match document.get("name") {
                        Some(&Bson::String(ref name)) => name,
                        _ => return Err(Error::Proddle(String::from("failed to parse parameter name as string"))),
                    };

                    let value = match document.get("value") {
                        Some(&Bson::String(ref value)) => value,
                        _ => return Err(Error::Proddle(String::from("operation: failed to parse parameter value as string"))),
                    };

                    hash_map.insert(name.to_owned(), value.to_owned());
                }

                Some(hash_map)
            },
            _ => return Err(Error::Proddle(String::from("failed to parse parameters as array"))),
        };

        let interval = match document.get("interval") {
            Some(&Bson::I32(interval)) => interval as u32,
            _ => return Err(Error::Proddle(String::from("failed to parse interval as i32"))),
        };

        let tags: Option<Vec<String>> = match document.get("tags") {
            Some(&Bson::Array(ref tags)) => Some(tags.iter().map(|x| x.to_string().replace("\"", "")).collect()),
            _ => return Err(Error::Proddle(String::from("failed to parse tags as array"))),
        };

        Ok(
            Operation {
                timestamp: timestamp,
                measurement: measurement,
                domain: domain,
                url: url,
                parameters: parameters,
                interval: interval,
                tags: tags,
            }
        )
    }

    pub fn from_capnproto(msg: &proddle_capnp::operation::Reader) -> Result<Operation, Error> {
        let timestamp = match msg.get_timestamp() {
            0 => None,
            _ => Some(msg.get_timestamp()),
        };

        let measurement = msg.get_measurement().unwrap().to_owned();
        let domain = msg.get_domain().unwrap().to_owned();
        let url = msg.get_url().unwrap().to_owned();
        let parameters = match msg.has_parameters() {
            true => {
                let mut hash_map = HashMap::new();
                for parameter in msg.get_parameters().unwrap().iter() {
                    let name = match parameter.get_name() {
                        Ok(name) => name,
                        Err(_) => return Err(Error::Proddle(String::from("failed to retrieve name from parameter"))),
                    };

                    let value = match parameter.get_value() {
                        Ok(value) => value,
                        Err(_) => return Err(Error::Proddle(String::from("failed to retrieve value from parameter"))),
                    };

                    hash_map.insert(name.to_owned(), value.to_owned());
                }

                Some(hash_map)
            },
            false  => None,
        };

        let interval = msg.get_interval();

        let tags = match msg.has_tags() {
            true => Some(msg.get_tags().unwrap().iter().map(|x| x.unwrap().to_string()).collect()),
            false => None,
        };

        Ok(
            Operation {
                timestamp: timestamp,
                measurement: measurement,
                domain: domain,
                url: url,
                parameters: parameters,
                interval: interval,
                tags: tags,
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
