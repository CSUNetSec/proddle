extern crate bson;

use bson::Bson;
use bson::ordered::OrderedDocument;

use error::Error;
use proddle_capnp;

use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct Operation {
    pub timestamp: Option<u64>,
    pub domain: String,
    pub measurement: String,
    pub interval: u32,
    pub tags: Option<Vec<String>>,
}

impl Operation {
    pub fn from_mongodb(document: &OrderedDocument) -> Result<Operation, Error> {
        let timestamp = match document.get("timestamp") {
            Some(&Bson::I64(timestamp)) => Some(timestamp as u64),
            _ => return Err(Error::Proddle("failed to parse timestamp as i64".to_owned())),
        };

        let domain = match document.get("domain") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err(Error::Proddle("failed to domain as string".to_owned())),
        };

        let measurement = match document.get("measurement") {
            Some(&Bson::String(ref name)) => name.to_owned(),
            _ => return Err(Error::Proddle("failed to parse measurement name as string".to_owned())),
        };

        let interval = match document.get("interval") {
            Some(&Bson::I32(interval)) => interval as u32,
            _ => return Err(Error::Proddle("failed to parse interval as i32".to_owned())),
        };

        let tags: Option<Vec<String>> = match document.get("tags") {
            Some(&Bson::Array(ref tags)) => Some(tags.iter().map(|x| x.to_string().replace("\"", "")).collect()),
            _ => return Err(Error::Proddle("failed to parse tags as array".to_owned())),
        };

        Ok(
            Operation {
                timestamp: timestamp,
                domain: domain,
                measurement: measurement,
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

        let domain = msg.get_domain().unwrap().to_owned();
        let measurement = msg.get_measurement().unwrap().to_owned();
        let interval = msg.get_interval();

        let tags = match msg.has_tags() {
            true => Some(msg.get_tags().unwrap().iter().map(|x| x.unwrap().to_string()).collect()),
            false => None,
        };

        Ok(
            Operation {
                timestamp: timestamp,
                domain: domain,
                measurement: measurement,
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
