extern crate bson;
extern crate capnp;
extern crate capnp_rpc;
#[macro_use]
extern crate gj;
extern crate gjio;
extern crate proddle;

use bson::Bson;
use capnp::capability::Promise;
use capnp_rpc::RpcSystem;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::rpc_twoparty_capnp::Side;
use gj::{EventLoop, TaskReaper, TaskSet};
use gjio::SocketListener;
use proddle::proddle_capnp::proddle::{GetModulesParams, GetModulesResults, GetOperationsParams, GetOperationsResults};
use proddle::proddle_capnp::proddle::{Client, Server, ToClient};

use std::collections::HashMap;
use std::collections::hash_map::Entry;
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
        let proddle = ToClient::new(ServerImpl::new("127.0.0.1", 27017)).from_server::<capnp_rpc::Server>();
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
    mongodb_host: String,
    mongodb_port: u16,
}

impl ServerImpl {
    pub fn new(mongodb_host: &str, mongodb_port: u16) -> ServerImpl {
        ServerImpl {
            mongodb_host: mongodb_host.to_owned(),
            mongodb_port: mongodb_port,
        }
    }
}

impl Server for ServerImpl {
    fn get_modules(&mut self, params: GetModulesParams<>, mut results: GetModulesResults<>) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        //let bucket_hashes = pry!(params.get_bucket_hashes());
        let param_modules = pry!(params.get_modules());
        
        //connect to mongodb
        let client = match proddle::get_mongodb_client(&self.mongodb_host, self.mongodb_port) {
            Ok(client) => client,
            Err(e) => return Promise::err(capnp::Error::failed(format!("failed to connect to mongodb: {}", e))),
        };

        //iterate over modules in mongodb and store in modules map
        let mut modules = HashMap::new();
        match proddle::find_modules(client.clone(), None, None, None, false) {
            Ok(cursor) => {
                for document in cursor {
                    let document = match document {
                        Ok(document) => document,
                        Err(e) => {
                            println!("failed to fetch document: {}", e);
                            continue;
                        }
                    };

                    //parse fields from document
                    let module_name = match document.get("name") {
                        Some(&Bson::String(ref name)) => name.to_owned(),
                        _ => panic!("failed to parse name as string"),
                    };

                    let version = match document.get("version") {
                        Some(&Bson::I32(version)) => version,
                        _ => panic!("failed to parse version as i32"),
                    };

                    //TODO parse the rest of fields

                    modules.insert(module_name.clone(), (module_name, version as u16));
                }
            },
            Err(e) => return Promise::err(capnp::Error::failed(format!("failed to retrieve modules: {}", e))),
        }

        //compare vantage module versions to mongodb versions
        for module in param_modules.iter() {
            let module_name = match module.get_name() {
                Ok(name) => name,
                Err(e) => panic!("failed to retrieve name from module: {}", e),
            };

            let entry = modules.entry(module_name.to_owned());
            if let Entry::Occupied(occupied_entry) = entry {
                //if vantage version is up to date remove from mongodb map
                if module.get_version() >= occupied_entry.get().1 {
                    occupied_entry.remove();
                }
            } else if let Entry::Vacant(vacant_entry) = entry {
                //if mongodb map doens't contain remove from vantage
                vacant_entry.insert((module_name.to_owned(), 0));
            }
        }

        //create results message
        let mut results_modules = results.get().init_modules(modules.len() as u32);
        for (i, tuple) in modules.values().enumerate() {
            let mut module = results_modules.borrow().get(i as u32);
            
            //TODO use correct values populated
            if let Err(e) = proddle::build_module(&mut module, None, None, &tuple.0, tuple.1, None, None) {
                println!("{}", e);
                continue;
            }
        }

        Promise::ok(())
        //Promise::err(capnp::Error::unimplemented("method not implemented".to_string()))
    }

    fn get_operations(&mut self, _: GetOperationsParams<>, _: GetOperationsResults<>) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented("method not implemented".to_string()))
    }
}
