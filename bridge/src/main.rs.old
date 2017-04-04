extern crate bson;
extern crate capnp;
#[macro_use]
extern crate capnp_rpc;
#[macro_use]
extern crate clap;
extern crate futures;
extern crate mongodb;
extern crate proddle;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate slog_scope;
extern crate slog_term;
extern crate serde_json;
extern crate tokio_core;
extern crate tokio_io;

use capnp_rpc::RpcSystem;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::rpc_twoparty_capnp::Side;
use clap::{App, ArgMatches};
use futures::{Future, Stream};
use mongodb::{Client, ClientOptions, ThreadedClient};
use proddle::ProddleError;
use proddle::proddle_capnp::proddle::ToClient;
use slog::{DrainExt, Logger};
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;

mod server;

use server::ServerImpl;

use std::net::SocketAddr;
use std::str::FromStr;

fn parse_args(matches: &ArgMatches) -> Result<(String, String, u16, String, String, String, String, String), ProddleError> {
    let bridge_ip_address = try!(value_t!(matches, "BRIDGE_IP_ADDRESS", String));
    let bridge_port = try!(value_t!(matches.value_of("BRIDGE_PORT"), u16));
    let bridge_address = format!("{}:{}", bridge_ip_address, bridge_port);
    let mongodb_ip_address = try!(value_t!(matches, "MONGODB_IP_ADDRESS", String));
    let mongodb_port = try!(value_t!(matches.value_of("MONGODB_PORT"), u16));
    let ca_file = try!(value_t!(matches.value_of("CA_FILE"), String));
    let certificate_file = try!(value_t!(matches.value_of("CERTIFICATE_FILE"), String));
    let key_file = try!(value_t!(matches.value_of("KEY_FILE"), String));
    let username = try!(value_t!(matches.value_of("USERNAME"), String));
    let password = try!(value_t!(matches.value_of("PASSWORD"), String));

    Ok((bridge_address, mongodb_ip_address, mongodb_port, ca_file, certificate_file, key_file, username, password))
}

pub fn main() {
    slog_scope::set_global_logger(Logger::root(slog_term::streamer().build().fuse(), o![]));
    let yaml = load_yaml!("args.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    //initialize bridge parameters
    info!("parsing command line arguments");
    let (bridge_address, mongodb_ip_address, mongodb_port, ca_file, certificate_file, key_file, username, password) = match parse_args(&matches) {
        Ok(args) => args,
        Err(e) => panic!("{}", e),
    };

    //pasre socket address
    let socket_addr = match SocketAddr::from_str(&bridge_address) {
        Ok(socket_addr) => socket_addr,
        Err(e) => panic!("failed to parse socket address: {}", e),
    };

    //initialize tokio core
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let socket = TcpListener::bind(&socket_addr, &handle).unwrap();

    //connect to mongodb
    let client_result = if ca_file.eq("") && certificate_file.eq("") && key_file.eq("") {
        Client::connect(&mongodb_ip_address, mongodb_port)
    } else {
        let client_options = ClientOptions::with_ssl(&ca_file, &certificate_file, &key_file, true);
        Client::connect_with_options(&mongodb_ip_address, mongodb_port, client_options)
    };

    let client = match client_result {
        Ok(client) => client,
        Err(e) => panic!("failed to connect to mongodb: {}", e),
    };

    //initialize proddle bridge
    info!("initializing bridge data strucutes");
    let proddle = ToClient::new(ServerImpl::new(client, username, password)).from_server::<capnp_rpc::Server>();
    
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
