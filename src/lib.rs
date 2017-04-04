extern crate bincode;
extern crate bson;
extern crate clap;
extern crate curl;
extern crate mongodb;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use bincode::Infinite;

mod error;

pub use self::error::ProddleError;

use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::TcpStream;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MessageType {
    Dummy,
    UpdateOperationsRequest,
    UpdateOperationsResponse,
    SendMeasurementsRequest,
    SendMeasurementsResponse,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub message_type: MessageType,
    pub update_operations_request: Option<HashMap<u64, u64>>,
    pub update_operations_response: Option<HashMap<u64, Vec<Operation>>>,
    pub send_measurements_request: Option<Vec<String>>,
    pub send_measurements_response: Option<Vec<usize>>,
}

impl Message {
    pub fn update_operations_request(operation_bucket_hashes: HashMap<u64, u64>) -> Message {
        Message {
            message_type: MessageType::UpdateOperationsRequest,
            update_operations_request: Some(operation_bucket_hashes),
            update_operations_response: None,
            send_measurements_request: None,
            send_measurements_response: None,
        }
    }

    pub fn update_operations_response(operation_buckets: HashMap<u64, Vec<Operation>>) -> Message {
        Message {
            message_type: MessageType::UpdateOperationsResponse,
            update_operations_request: None,
            update_operations_response: Some(operation_buckets),
            send_measurements_request: None,
            send_measurements_response: None,
        }
    }

    pub fn send_measurements_request(measurements: Vec<String>) -> Message {
        Message {
            message_type: MessageType::SendMeasurementsRequest,
            update_operations_request: None,
            update_operations_response: None,
            send_measurements_request: Some(measurements),
            send_measurements_response: None,
        }
    }

    pub fn send_measurements_response(measurement_failures: Vec<usize>) -> Message {
        Message {
            message_type: MessageType::SendMeasurementsResponse,
            update_operations_request: None,
            update_operations_response: None,
            send_measurements_request: None,
            send_measurements_response: Some(measurement_failures),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Operation {
    pub timestamp: i64,
    pub measurement_class: String,
    pub domain: String,
    pub parameters: Vec<Parameter>,
    pub tags: Vec<String>,
}

impl Hash for Operation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.measurement_class.hash(state);
        self.domain.hash(state);

        for parameter in self.parameters.iter() {
            parameter.name.hash(state);
            parameter.value.hash(state);
        }

        for tag in self.tags.iter() {
            tag.hash(state);
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Parameter {
    pub name: String,
    pub value: String,
}

pub fn message_to_stream(message: &Message, stream: &mut TcpStream) -> Result<(), ProddleError> {
    let encoded: Vec<u8> = bincode::serialize(message, Infinite).unwrap();
    let length = 4 + encoded.len() as u32;

    try!(stream.write(&[(length as u8), ((length >> 8) as u8), ((length >> 16) as u8), ((length >> 24) as u8)]));
    try!(stream.write_all(&encoded));
    try!(stream.flush());

    Ok(())
}

pub fn message_from_stream(buf: &mut Vec<u8>, stream: &mut TcpStream) -> Result<Message, ProddleError> {
    //read from stream
    let mut bytes_read = 0;
    while bytes_read < 4 {
        bytes_read += try!(stream.read(&mut buf[0..4]));
    }

    //decode length
    let length = ((buf[0] as u32) | ((buf[1] as u32) << 8) | ((buf[2] as u32) << 16) | ((buf[3] as u32) << 24)) as usize;
    while bytes_read < length {
        bytes_read += try!(stream.read(&mut buf[bytes_read..length]));
    }

    let message = bincode::deserialize(&buf[4..bytes_read]).unwrap();
    Ok(message)
}
