use bson::{self, Document};
use proddle::{self, Message, MessageType, ProddleError};

use operation_job::OperationJob;

use std::collections::{BinaryHeap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

pub struct Client {
    socket_addr: SocketAddr,
}

impl Client {
    pub fn new(socket_addr: SocketAddr) -> Client {
        Client {
            socket_addr: socket_addr,
        }
    }

    pub fn send_measurements(&mut self, measurement_buffer: &mut Vec<Document>) -> Result<(), ProddleError> {
        //open stream
        let mut stream = try!(TcpStream::connect(self.socket_addr));
        try!(stream.set_read_timeout(Some(Duration::new(180, 0))));
        try!(stream.set_write_timeout(Some(Duration::new(180, 0))));

        //create request
        let mut measurements = Vec::new();
        for measurement in measurement_buffer.iter() {
            let mut encoded = Vec::new();
            try!(bson::encode_document(&mut encoded, measurement));
            measurements.push(encoded);
        }
        let request = Message::send_measurements_request(measurements);

        //send request and recv response
        try!(proddle::message_to_stream(&request, &mut stream));
        let response = try!(proddle::message_from_stream(&mut stream));
        match response.message_type {
            MessageType::Error => {
                match response.error {
                    Some(error) => Err(ProddleError::from(error)),
                    None => Err(ProddleError::from("malformed error message in send measurements")),
                }
            },
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
        try!(stream.set_read_timeout(Some(Duration::new(180, 0))));
        try!(stream.set_write_timeout(Some(Duration::new(180, 0))));

        //create request
        let request = Message::update_operations_request(operation_bucket_hashes.clone());

        //send request and recv response
        try!(proddle::message_to_stream(&request, &mut stream));
        let response = try!(proddle::message_from_stream(&mut stream));
        match response.message_type {
            MessageType::Error => {
                match response.error {
                    Some(error) => Err(ProddleError::from(error)),
                    None => Err(ProddleError::from("malformed error message in update operations")),
                }
            },
            MessageType::UpdateOperationsResponse => {
                match response.update_operations_response {
                    Some(operation_buckets) => {
                        let mut updated_operations_count = 0;

                        //iterate over operation buckets
                        for (bucket_key, operation_vec) in operation_buckets.iter() {
                            let mut binary_heap = BinaryHeap::new();
                            let mut hasher = DefaultHasher::new();
                            for operation in operation_vec {
                                operation.hash(&mut hasher);

                                //validate tags
                                let mut operation_interval = i64::max_value();
                                //check if tag is in exclude tags
                                let mut found = false;
                                for operation_tag in operation.tags.iter() {
                                    for exclude_tag in exclude_tags {
                                        if operation_tag.eq(*exclude_tag) {
                                            found = true;
                                        }
                                    }
                                }

                                if found {
                                    continue
                                }

                                //determine interval
                                for operation_tag in operation.tags.iter() {
                                    for (include_tag, interval) in include_tags {
                                        if operation_tag.eq(*include_tag) && *interval < operation_interval {
                                            operation_interval = *interval;
                                        }
                                    }
                                }

                                //check if include tag interval was found
                                if operation_interval == i64::max_value() {
                                    continue;
                                }

                                //add operation
                                binary_heap.push(OperationJob::new(operation.to_owned(), operation_interval));
                                updated_operations_count += 1;
                            }

                            //insert new operations into operations map
                            operations.insert(*bucket_key, binary_heap);
                            operation_bucket_hashes.insert(*bucket_key, hasher.finish());
                        }
                        Ok(updated_operations_count)
                    },
                    None => Err(ProddleError::from("malformed update opertions respose.")),
                }
            },
            _ => Err(ProddleError::from("failed to receive UpdateOperationsResponse.")),
        }
    }
}
