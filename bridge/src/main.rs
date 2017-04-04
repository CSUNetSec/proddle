extern crate bson;
#[macro_use]
extern crate chan;
#[macro_use]
extern crate clap;
extern crate mongodb;
extern crate proddle;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate slog_scope;
extern crate slog_term;
extern crate serde_json;

use chan::Receiver;
use clap::{App, ArgMatches};
use proddle::{Message, MessageType, ProddleError};
use slog::{DrainExt, Logger};

mod db_wrapper;
use db_wrapper::DbWrapper;

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

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
    let db_wrapper = match DbWrapper::new(&mongodb_ip_address, mongodb_port, &username, 
                                                  &password, &ca_file, &certificate_file, &key_file) {
        Ok(db_wrapper) => Arc::new(RwLock::new(db_wrapper)),
        Err(e) => panic!("failed to initialize db_wrapper: {}", e),
    };

    //start stream threadpool
    let (stream_tx, stream_rx) = chan::sync(0);
    for _ in 0..8 {
        let t_stream_rx: Receiver<TcpStream> = stream_rx.clone();
        let t_db_wrapper = db_wrapper.clone();
        let _ = std::thread::spawn(move || {
            loop {
                chan_select! {
                    t_stream_rx.recv() -> stream => {
                        match stream {
                            Some(mut stream) => {
                                let db_wrapper = t_db_wrapper.read().unwrap();
                                if let Err(e) = handle_stream(&mut stream, &db_wrapper) {
                                    error!("{}", e);
                                }
                            },
                            None => error!("failed to recv stream"),
                        }
                    },
                }
            }
        });
    }

    //start listener
    let listener = match TcpListener::bind(socket_addr) {
        Ok(listener) => listener,
        Err(e) => panic!("failed to bind to address '{}': {}", socket_addr, e),
    };

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => stream_tx.send(stream),
            Err(e) => error!("recv connection failed: {}", e),
        }
    }
}

fn handle_stream(stream: &mut TcpStream, db_wrapper: &DbWrapper) -> Result<(), ProddleError> {
    let request = try!(proddle::message_from_stream(stream));
    match request.message_type {
        MessageType::SendMeasurementsRequest => {
            match request.send_measurements_request {
                Some(measurements) => {
                    //attempt to send measurements to db
                    let measurement_count = measurements.len();
                    let message = match db_wrapper.send_measurements(measurements) {
                        Ok(measurement_failures) => {
                            info!("{}: inserted {} measurement(s), {} measurement(s) failed", stream.peer_addr().unwrap(), 
                                measurement_count - measurement_failures.len(), measurement_failures.len());
                            Message::send_measurements_response(measurement_failures)
                        },
                        Err(e) => Message::error(format!("{}", e)),
                    };

                    //send response
                    try!(proddle::message_to_stream(&message, stream));
                    Ok(())
                },
                None => Err(ProddleError::from("recv malformed send measurements request")),
            }
        },
        MessageType::UpdateOperationsRequest => {
            match request.update_operations_request {
                Some(operation_bucket_hashes) => {
                    //attempt to update operations from db
                    let message = match db_wrapper.update_operations(operation_bucket_hashes) {
                        Ok(operation_buckets) => {
                            if operation_buckets.len() > 0 {
                                info!("{}: updated {} operation bucket(s)", stream.peer_addr().unwrap(), operation_buckets.len());
                            }
                            Message::update_operations_response(operation_buckets)
                        },
                        Err(e) => Message::error(format!("{}", e)),
                    };

                    //send response
                    try!(proddle::message_to_stream(&message, stream));
                    Ok(())
                },
                None => Err(ProddleError::from("recv malformed update operations request")),
            }
        },
        _ => Err(ProddleError::from(format!("unsupported message type: '{:?}'", request.message_type)))
    }
}
