extern crate capnp;
extern crate capnp_rpc;
extern crate gj;
extern crate gjio;
extern crate proddle;

use capnp::capability::Promise;
use capnp_rpc::RpcSystem;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::rpc_twoparty_capnp::Side;
use gj::{EventLoop, TaskReaper, TaskSet};
use gjio::SocketListener;
use proddle::proddle_capnp::proddle::{GetModulesParams, GetModulesResults, GetOperationsParams, GetOperationsResults};
use proddle::proddle_capnp::proddle::{Client, Server, ToClient};

use std::net::SocketAddr;
use std::str::FromStr;

fn main() {
    let result = EventLoop::top_level(move |wait_scope| -> Result<(), capnp::Error> {
        //open tcp listener
        let mut event_port = try!(gjio::EventPort::new());
        let socket_addr = match SocketAddr::from_str(&format!("127.0.0.1:12289")) {
            Ok(socket_addr) => socket_addr,
            Err(e) => panic!("failed to parse socket address: {}", e),
        };

        let mut tcp_address = event_port.get_network().get_tcp_address(socket_addr);
        let listener = try!(tcp_address.listen());

        //start server
        let proddle = ToClient::new(ServerImpl::new()).from_server::<capnp_rpc::Server>();
        let task_set = TaskSet::new(Box::new(Reaper));

        //accept connection
        try!(accept_loop(listener, task_set, proddle).wait(wait_scope, &mut event_port));

        Ok(())
    });

    if let Err(e) = result {
        panic!("event loop failed: {}", e);
    }
}

struct Reaper;

impl TaskReaper<(), capnp::Error> for Reaper {
    fn task_failed(&mut self, error: capnp::Error) {
        println!("task failed: {}", error)
    }
}

fn accept_loop(listener: SocketListener, mut task_set: TaskSet<(), capnp::Error>, proddle: Client) -> Promise<(), std::io::Error> {
    //TODO understand this code - right now it's a black box
    listener.accept().then(move |stream| {
        let mut network = VatNetwork::new(stream.clone(), stream, Side::Server, Default::default());
        let disconnect_promise = network.on_disconnect();

        let rpc_system = RpcSystem::new(Box::new(network), Some(proddle.clone().client));

        task_set.add(disconnect_promise.attach(rpc_system));
        accept_loop(listener, task_set, proddle)
    })
}

struct ServerImpl {
}

impl ServerImpl {
    pub fn new() -> ServerImpl {
        ServerImpl {
        }
    }
}

impl Server for ServerImpl {
    fn get_modules(&mut self, _: GetModulesParams<>, _: GetModulesResults<>) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented("method not implemented".to_string()))
    }

    fn get_operations(&mut self, _: GetOperationsParams<>, _: GetOperationsResults<>) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented("method not implemented".to_string()))
    }
}
