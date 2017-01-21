extern crate bson;

use bson::Bson;
use bson::ordered::OrderedDocument;

use error::Error;
use proddle_capnp;

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
