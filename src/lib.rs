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

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::TcpStream;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MessageType {
    Dummy,
    Error,
    UpdateOperationsRequest,
    UpdateOperationsResponse,
    SendMeasurementsRequest,
    SendMeasurementsResponse,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub message_type: MessageType,
    pub error: Option<String>,
    pub update_operations_request: Option<HashMap<u64, u64>>,
    pub update_operations_response: Option<HashMap<u64, Vec<Operation>>>,
    pub send_measurements_request: Option<Vec<Vec<u8>>>,
    pub send_measurements_response: Option<Vec<usize>>,
}

impl Message {
    pub fn error(error: String) -> Message {
        Message {
            message_type: MessageType::Error,
            error: Some(error),
            update_operations_request: None,
            update_operations_response: None,
            send_measurements_request: None,
            send_measurements_response: None,
        }
    }

    pub fn update_operations_request(operation_bucket_hashes: HashMap<u64, u64>) -> Message {
        Message {
            message_type: MessageType::UpdateOperationsRequest,
            error: None,
            update_operations_request: Some(operation_bucket_hashes),
            update_operations_response: None,
            send_measurements_request: None,
            send_measurements_response: None,
        }
    }

    pub fn update_operations_response(operation_buckets: HashMap<u64, Vec<Operation>>) -> Message {
        Message {
            message_type: MessageType::UpdateOperationsResponse,
            error: None,
            update_operations_request: None,
            update_operations_response: Some(operation_buckets),
            send_measurements_request: None,
            send_measurements_response: None,
        }
    }

    pub fn send_measurements_request(measurements: Vec<Vec<u8>>) -> Message {
        Message {
            message_type: MessageType::SendMeasurementsRequest,
            error: None,
            update_operations_request: None,
            update_operations_response: None,
            send_measurements_request: Some(measurements),
            send_measurements_response: None,
        }
    }

    pub fn send_measurements_response(measurement_failures: Vec<usize>) -> Message {
        Message {
            message_type: MessageType::SendMeasurementsResponse,
            error: None,
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
    let length = encoded.len() as u32;

    try!(stream.write(&[(length as u8), ((length >> 8) as u8), ((length >> 16) as u8), ((length >> 24) as u8)]));
    try!(stream.write_all(&encoded));
    try!(stream.flush());

    Ok(())
}

pub fn message_from_stream(stream: &mut TcpStream) -> Result<Message, ProddleError> {
    let mut length_buffer = vec![0u8; 4];
    try!(stream.read_exact(&mut length_buffer));
    let length = ((length_buffer[0] as u32) | ((length_buffer[1] as u32) << 8) | ((length_buffer[2] as u32) << 16) | ((length_buffer[3] as u32) << 24)) as usize;

    let mut byte_buffer = vec![0u8; length];
    try!(stream.read_exact(&mut byte_buffer));
    let message = try!(bincode::deserialize(&byte_buffer));
    Ok(message)
}
