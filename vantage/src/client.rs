use bson::Bson;
use futures::{BoxFuture, Future};
use proddle::{Message, ProddleError, ProddleProto, Operation};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use tokio_proto::TcpClient;
use tokio_proto::pipeline::{ClientProto, ClientService};
use tokio_service::Service;

use operation_job::OperationJob;

use std;
use std::collections::{BinaryHeap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::str::FromStr;

pub struct Client {
    core: Core,
    socket_addr: SocketAddr,
}

impl Client {
    pub fn new(socket_addr: SocketAddr) -> Client {
        Client {
            core: Core::new().unwrap(),
            socket_addr: socket_addr,
        }
    }

    pub fn send_measurements(&mut self, measurement_buffer: &mut Vec<Bson>) -> Result<(), ProddleError> {
        let handle = self.core.handle();
        let clientHandler = try!(self.core.run(ClientHandle::connect(&self.socket_addr, &handle)));

        let message = Message::new_update_operations_request();
        try!(self.core.run(clientHandler.call(message)));
        Ok(())
    }

    pub fn update_operations(&mut self, operations: &mut HashMap<u64, BinaryHeap<OperationJob>>, 
                             operation_bucket_hashes: &mut HashMap<u64, u64>, include_tags: &HashMap<&str, i64>, 
                             exclude_tags: &Vec<&str>) -> Result<i32, ProddleError> {
        let mut handle = self.core.handle();
        let clientHandler = try!(self.core.run(ClientHandle::connect(&self.socket_addr, &handle)));

        let message = Message::new_update_operations_request();
        try!(self.core.run(clientHandler.call(message)));
        let clientHandler = try!(self.core.run(ClientHandle::connect(&self.socket_addr, &handle)));
        Ok(0)
    }
}

pub struct ClientHandle {
    inner: ClientService<TcpStream, ProddleProto>,
}

impl ClientHandle {
    pub fn connect(addr: &SocketAddr, handle: &Handle) -> Box<Future<Item = ClientHandle, Error = std::io::Error>> {
        let ret = TcpClient::new(ProddleProto)
            .connect(addr, handle)
            .map(|client_service| {
                ClientHandle { inner: client_service }
            });

        Box::new(ret)
    }
}

impl Service for ClientHandle {
    type Request = Message;
    type Response = Message;
    type Error = std::io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        self.inner.call(req).boxed()
    }
}
