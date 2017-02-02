extern crate env_logger;
extern crate capnp;
#[macro_use]
extern crate capnp_rpc;
#[macro_use]
extern crate clap;
extern crate futures;
#[macro_use]
extern crate log;
extern crate mongodb;
extern crate proddle;
extern crate tokio_core;

use capnp_rpc::RpcSystem;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::rpc_twoparty_capnp::Side;
use clap::App;
use futures::{Future, Stream};
use mongodb::{Client, ThreadedClient};
use proddle::proddle_capnp::proddle::ToClient;
use tokio_core::net::TcpListener;
use tokio_core::io::Io;
use tokio_core::reactor::Core;

mod server;

use server::ServerImpl;

use std::net::SocketAddr;
use std::str::FromStr;

pub fn main() {
    env_logger::init().unwrap();
    let yaml = load_yaml!("args.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    //initialize bridge parameters
    info!("parsing command line arguments");
    let listen_socket_address = format!("{}:{}", matches.value_of("BRIDGE_IP_ADDRESS").unwrap(), matches.value_of("BRIDGE_PORT").unwrap());
    let mongodb_ip_address = matches.value_of("MONGODB_IP_ADDRESS").unwrap();
    let mongodb_port = match matches.value_of("MONGODB_PORT").unwrap().parse::<u16>() {
        Ok(mongodb_port) => mongodb_port,
        Err(e) => panic!("failed to parse mongodb_port as u16: {}", e),
    };

    //pasre socket address
    let socket_addr = match SocketAddr::from_str(&listen_socket_address) {
        Ok(socket_addr) => socket_addr,
        Err(e) => panic!("failed to parse socket address: {}", e),
    };

    //initialize tokio core
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let socket = TcpListener::bind(&socket_addr, &handle).unwrap();

    //connect to mongodb
    let client = match Client::connect(mongodb_ip_address, mongodb_port)  {
        Ok(client) => client,
        Err(e) => panic!("failed to connect to mongodb: {}", e),
    };

    //initialize proddle bridge
    info!("initializing bridge data strucutes");
    let proddle = ToClient::new(ServerImpl::new(client)).from_server::<capnp_rpc::Server>();
    
    //start rpc loop
    info!("service started");
    let done = socket.incoming().for_each(move |(socket, _addr)| {
        try!(socket.set_nodelay(true));
        let (reader, writer) = socket.split();

        let handle = handle.clone();
        let network = VatNetwork::new(reader, writer, Side::Server, Default::default());
        let rpc_system = RpcSystem::new(Box::new(network), Some(proddle.clone().client));
        handle.spawn(rpc_system.map_err(|e| error!("{:?}", e)));
        
        Ok(())
    });

    core.run(done).unwrap();
}
