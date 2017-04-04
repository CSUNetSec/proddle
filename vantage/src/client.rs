use bson::Bson;
use proddle::{self, Message, MessageType, ProddleError, Operation};

use operation_job::OperationJob;

use std;
use std::collections::{BinaryHeap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, TcpStream};
use std::str::FromStr;

pub struct Client {
    socket_addr: SocketAddr,
}

impl Client {
    pub fn new(socket_addr: SocketAddr) -> Client {
        Client {
            socket_addr: socket_addr,
        }
    }

    pub fn send_measurements(&mut self, measurement_buffer: &mut Vec<Bson>) -> Result<(), ProddleError> {
        //open stream
        let mut stream = try!(TcpStream::connect(self.socket_addr));

        //create request
        let measurements: Vec<String> = measurement_buffer.iter().map(|bson| bson.to_json().to_string()).collect();
        let request = Message::send_measurements_request(measurements);

        //send request and recv response
        try!(proddle::message_to_stream(&request, &mut stream));
        let response = try!(proddle::message_from_stream(&mut stream));
        match response.message_type {
            MessageType::SendMeasurementsResponse => {
                //TODO handle send measurements response
                measurement_buffer.clear();
                Ok(())
            },
            _ => Err(ProddleError::from("failed to receive SendMeasurementsResponse."))
        }
    }

    pub fn update_operations(&mut self, operations: &mut HashMap<u64, BinaryHeap<OperationJob>>, 
                             operation_bucket_hashes: &mut HashMap<u64, u64>, include_tags: &HashMap<&str, i64>, 
                             exclude_tags: &Vec<&str>) -> Result<i32, ProddleError> {
        //open stream
        let mut stream = try!(TcpStream::connect(self.socket_addr));

        //create request
        let request = Message::update_operations_request(operation_bucket_hashes.clone());

        //send request and recv response
        try!(proddle::message_to_stream(&request, &mut stream));
        let response = try!(proddle::message_from_stream(&mut stream));
        match response.message_type {
            MessageType::UpdateOperationsResponse => {
                let mut updated_operations_count = 0;
                //TODO handle send measurements response
                Ok(updated_operations_count)
            },
            _ => Err(ProddleError::from("failed to receive UpdateOperationsResponse."))
        }
    }
}
