extern crate capnp;
extern crate capnp_rpc;
extern crate gj;
extern crate gjio;
extern crate proddle;

use capnp_rpc::RpcSystem;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::rpc_twoparty_capnp::Side;
use gj::EventLoop;
use proddle::Module;
use proddle::proddle_capnp::proddle::Client;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;

fn main() {
    let result = EventLoop::top_level(move |wait_scope| -> Result<(), capnp::Error> {
        //open stream
        let mut event_port = try!(gjio::EventPort::new());
        let socket_addr = match SocketAddr::from_str(&format!("127.0.0.1:12289")) {
            Ok(socket_addr) => socket_addr,
            Err(e) => panic!("failed to parse socket address: {}", e),
        };

        let tcp_address = event_port.get_network().get_tcp_address(socket_addr);
        let stream = try!(tcp_address.connect().wait(wait_scope, &mut event_port));

        //connect rpc client
        let network = Box::new(VatNetwork::new(stream.clone(), stream, Side::Client, Default::default()));
        let mut rpc_system = RpcSystem::new(network, None);
        let proddle: Client = rpc_system.bootstrap(Side::Server);

        //TODO execute command for client
        let mut request = proddle.get_modules_request();
        {
            let mut request_modules = request.get().init_modules(2);
            /*for module in 0..module_count {
                let mut bucket_hash = bucket_hashes.borrow().get(bucket);
                bucket_hash.set_bucket(0);
                bucket_hash.set_hash(0);
            }*/
            {   
                let mut request_module = request_modules.borrow().get(0);
                request_module.set_name("core/veil.py");
                request_module.set_version(1);
            }

            let mut request_module = request_modules.borrow().get(1);
            request_module.set_name("core/shadow.py");
            request_module.set_version(2);
        }

        let response = try!(request.send().promise.wait(wait_scope, &mut event_port));
        let reader = try!(response.get());
        let result_modules = try!(reader.get_modules());

        for result_module in result_modules.iter() {
            let module = match Module::from_capnproto(&result_module) {
                Ok(module) => module,
                Err(e) => panic!("failed to parse capnproto to module: {}", e),
            };

            println!("PROCESSING MODULE {},{}", module.name, module.version);
        }

        Ok(())
    });

    if let Err(e) = result {
        panic!("event loop failed: {}", e);
    }
}
