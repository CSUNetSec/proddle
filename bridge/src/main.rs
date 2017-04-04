extern crate bson;
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
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;

use clap::{App, ArgMatches};
use futures::{BoxFuture, Future};
use proddle::{Message, ProddleError, ProddleProto};
use slog::{DrainExt, Logger};
use tokio_proto::TcpServer;
use tokio_service::Service;

mod mongodb_client;

use mongodb_client::MongodbClient;

use std::net::SocketAddr;
use std::str::FromStr;

fn parse_args(matches: &ArgMatches) -> Result<(SocketAddr, String, u16, String, String, String, String, String), ProddleError> {
    let bridge_ip_address = try!(value_t!(matches, "BRIDGE_IP_ADDRESS", String));
    let bridge_port = try!(value_t!(matches.value_of("BRIDGE_PORT"), u16));
    let bridge_address = try!(SocketAddr::from_str(&format!("{}:{}", bridge_ip_address, bridge_port)));
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
    let (socket_addr, mongodb_ip_address, mongodb_port, ca_file, certificate_file, key_file, username, password) = match parse_args(&matches) {
        Ok(args) => args,
        Err(e) => panic!("{}", e),
    };

    //connect to mongodb client
    let mongodb_client = match MongodbClient::new(&mongodb_ip_address, mongodb_port, &username, 
                                                  &password, &ca_file, &certificate_file, &key_file) {
        Ok(mongodb_client) => mongodb_client,
        Err(e) => panic!("failed to connect to mongodb: {}", e),
    };

    //start bridge
    let server = TcpServer::new(ProddleProto, socket_addr);
    server.serve(|| Ok(Bridge));
}

struct Bridge;

impl Service for Bridge {
    type Request = Message;
    type Response = Message;
    type Error = ProddleError;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        println!("req message type: {:?}", req.message_type);
        futures::future::ok(req).boxed()
    }
}
