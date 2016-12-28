extern crate bson;
extern crate capnp;
extern crate capnp_rpc;
#[macro_use]
extern crate gj;
extern crate gjio;
extern crate proddle;

use capnp::capability::Promise;
use capnp_rpc::RpcSystem;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::rpc_twoparty_capnp::Side;
use gj::{EventLoop, TaskReaper, TaskSet};
use gjio::SocketListener;
use proddle::Module;
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

                    //parse mongodb document into module
                    let module = match Module::from_mongodb(&document) {
                        Ok(module) => module,
                        Err(e) => panic!("failed to parse mongodb document into module: {}", e),
                    };

                    //check if module name already exists and/or version comparrison
                    let entry = modules.entry(module.name.to_owned());
                    if let Entry::Occupied(mut occupied_entry) = entry {
                        let replace;
                        {
                            let entry: &Module = occupied_entry.get();
                            replace = module.version > entry.version;
                        }
                        
                        //if version is newer then replace
                        if replace {
                            occupied_entry.insert(module);
                        }
                    } else if let Entry::Vacant(vacant_entry) = entry {
                        //if entry does not exist then insert
                        vacant_entry.insert(module);
                    }
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
                if module.get_version() >= occupied_entry.get().version {
                    occupied_entry.remove();
                }
            } else if let Entry::Vacant(vacant_entry) = entry {
                //if mongodb map doens't contain remove from vantage
                vacant_entry.insert(Module::new(None, module_name.to_owned(), 0, None, None));
            }
        }

        //create results message
        let mut results_modules = results.get().init_modules(modules.len() as u32);
        for (i, entry) in modules.values().enumerate() {
            //populate module
            let mut module = results_modules.borrow().get(i as u32);

            if let Some(timestamp) = entry.timestamp {
                module.set_timestamp(timestamp);
            }

            module.set_name(&entry.name);
            module.set_version(entry.version);

            if let Some(ref dependencies) = entry.dependencies {
                let mut module_dependencies = module.borrow().init_dependencies(dependencies.len() as u32);
                for (i, dependency) in dependencies.iter().enumerate() {
                    module_dependencies.set(i as u32, dependency);
                }
            }

            if let Some(ref content) = entry.content {
                module.set_content(&content);
            }
        }

        Promise::ok(())
    }

    fn get_operations(&mut self, _: GetOperationsParams<>, _: GetOperationsResults<>) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented("method not implemented".to_string()))
    }
}
