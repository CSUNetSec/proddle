extern crate bson;

use bson::Bson;
use bson::ordered::OrderedDocument;

use error::ProddleError;
use proddle_capnp;

use std::collections::HashMap;

pub struct Measurement {
    pub timestamp: Option<u64>,
    pub name: String,
    pub version: u16,
    pub parameters: Option<HashMap<String, String>>,
    pub dependencies: Option<Vec<String>>,
    pub content: Option<String>,
}

impl Measurement {
    pub fn new(timestamp: Option<u64>, name: String, version: u16, parameters: Option<HashMap<String, String>>, dependencies: Option<Vec<String>>, content: Option<String>) -> Measurement {
        Measurement {
            timestamp: timestamp,
            name: name,
            version: version,
            parameters: parameters,
            dependencies: dependencies,
            content: content,
        }
    }

    pub fn from_mongodb(document: &OrderedDocument) -> Result<Measurement, ProddleError> {
        let timestamp = match document.get("timestamp") {
            Some(&Bson::I64(timestamp)) => Some(timestamp as u64),
            _ => return Err(ProddleError::Proddle(String::from("failed to parse timestamp as i64"))),
        };

        let measurement_name = match document.get("name") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err(ProddleError::Proddle(String::from("failed to parse name as string"))),
        };

        let version = match document.get("version") {
            Some(&Bson::I32(version)) => version as u16,
            _ => return Err(ProddleError::Proddle(String::from("failed to parse version as i32"))),
        };

        let parameters: Option<HashMap<String, String>> = match document.get("parameters") {
            Some(&Bson::Array(ref parameters)) => {
                let mut hash_map = HashMap::new();
                for parameter in parameters.iter() {
                    let document = match parameter {
                        &Bson::Document(ref document) => document,
                        _ => return Err(ProddleError::Proddle(String::from("failed to parameter name as bson document"))),
                    };

                    let name = match document.get("name") {
                        Some(&Bson::String(ref name)) => name,
                        _ => return Err(ProddleError::Proddle(String::from("failed to parse parameter name as string"))),
                    };

                    let value = match document.get("value") {
                        Some(&Bson::String(ref value)) => value,
                        _ => return Err(ProddleError::Proddle(String::from("mongodb: failed to parse parameter value as string"))),
                    };

                    hash_map.insert(name.to_owned(), value.to_owned());
                }

                Some(hash_map)
            },
            _ => return Err(ProddleError::Proddle(String::from("failed to parse parameters as array"))),
        };

        let dependencies: Option<Vec<String>> = match document.get("dependencies") {
            Some(&Bson::Array(ref dependencies)) => Some(dependencies.iter().map(|x| x.to_string().replace("\"", "")).collect()),
            _ => return Err(ProddleError::Proddle(String::from("failed to parse dependencies as array"))),
        };
        
        let content = match document.get("content") {
            Some(&Bson::String(ref content)) => Some(content.to_owned()),
            _ => return Err(ProddleError::Proddle(String::from("failed to parse content as string"))),
        };

        Ok(
            Measurement {
                timestamp: timestamp,
                name: measurement_name,
                version: version,
                parameters:  parameters,
                dependencies: dependencies,
                content: content,
            }
        )
    }

    pub fn from_capnproto(msg: &proddle_capnp::measurement::Reader) -> Result<Measurement, ProddleError> {
        let timestamp = match msg.get_timestamp() {
            0 => None,
            _ => Some(msg.get_timestamp()),
        };

        let measurement_name = msg.get_name().unwrap().to_owned();
        let version = msg.get_version();

        let parameters = match msg.has_parameters() {
            true => {
                let mut hash_map = HashMap::new();
                for parameter in msg.get_parameters().unwrap().iter() {
                    let name = match parameter.get_name() {
                        Ok(name) => name,
                        Err(_) => return Err(ProddleError::Proddle(String::from("failed to retrieve name from parameter"))),
                    };

                    let value = match parameter.get_value() {
                        Ok(value) => value,
                        Err(_) => return Err(ProddleError::Proddle(String::from("failed to retrieve value from parameter"))),
                    };

                    hash_map.insert(name.to_owned(), value.to_owned());
                }

                Some(hash_map)
            },
            false  => None,
        };

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
                parameters: parameters,
                dependencies: dependencies,
                content: content,
            }
        )
    }
}
