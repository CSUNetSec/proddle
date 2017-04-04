use bson::Bson;
use proddle::{Message, ProddleError, ProddleProto, Operation};

use operation_job::OperationJob;

use std;
use std::collections::{BinaryHeap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
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
        /*match TcpStream::connect(socket_addr) {
            Ok(mut stream) => {
                proddle::message_to_stream(&message, &mut stream);
                match proddle::message_from_stream(&mut buf, &mut  stream) {
                    Ok(Some(message)) => println!("recv response: {:?}", message),
                    _ => println!("unable to decode message"),
                }
            },
            Err(e) => println!("unable to connect"),
        };*/

        Ok(())
    }

    pub fn update_operations(&mut self, operations: &mut HashMap<u64, BinaryHeap<OperationJob>>, 
                             operation_bucket_hashes: &mut HashMap<u64, u64>, include_tags: &HashMap<&str, i64>, 
                             exclude_tags: &Vec<&str>) -> Result<i32, ProddleError> {
        Ok(0)
    }
}
