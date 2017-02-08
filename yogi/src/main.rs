#[macro_use(bson, doc)]
extern crate bson;
#[macro_use]
extern crate clap;
extern crate mongodb;
extern crate proddle;
extern crate rand;
extern crate rustc_serialize;
extern crate time;

use clap::{App, ArgMatches};
use mongodb::{Client, ClientOptions, ThreadedClient};
use proddle::Error;

mod measurement;
mod operation;

fn parse_args(matches: &ArgMatches) -> Result<(String, u16, String, String, String), Error> {
    let mongodb_ip_address = try!(value_t!(matches, "MONGODB_IP_ADDRESS", String));
    let mongodb_port = try!(value_t!(matches.value_of("MONGODB_PORT"), u16));
    let ca_file = try!(value_t!(matches.value_of("CA_FILE"), String));
    let certificate_file = try!(value_t!(matches.value_of("CERTIFICATE_FILE"), String));
    let key_file = try!(value_t!(matches.value_of("KEY_FILE"), String));

    Ok((mongodb_ip_address, mongodb_port, ca_file, certificate_file, key_file))
}

fn main() {
    let yaml = load_yaml!("args.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    //initialize bridge parameters
    let (mongodb_ip_address, mongodb_port, ca_file, certificate_file, key_file) = match parse_args(&matches) {
        Ok(args) => args,
        Err(e) => panic!("{}", e),
    };

    //connect to mongodb
    let client_options = ClientOptions::with_ssl(&ca_file, &certificate_file, &key_file, true);
    let client = match Client::connect_with_options(&mongodb_ip_address, mongodb_port, client_options)  {
        Ok(client) => client,
        Err(e) => panic!("{}", e),
    };

    let result = if let Some(matches) = matches.subcommand_matches("measurement") {
        if let Some(matches) = matches.subcommand_matches("add") {
            measurement::add(client, matches)
        } else if let Some(matches) = matches.subcommand_matches("delete") {
            measurement::delete(client, matches)
        } else if let Some(matches) = matches.subcommand_matches("search") {
            measurement::search(client, matches)
        } else {
            panic!("measurement unreachable");
        }
    } else if let Some(matches) = matches.subcommand_matches("operation") {
        if let Some(matches) = matches.subcommand_matches("add") {
            operation::add(client, matches)
        } else if let Some(matches) = matches.subcommand_matches("delete") {
            operation::delete(client, matches)
        } else if let Some(matches) = matches.subcommand_matches("search") {
            operation::search(client, matches)
        } else {
            panic!("operation unreachable");
        }
    } else {
        panic!("unreachable");
    };

    if let Err(e) = result {
        panic!("{}", e);
    }
}
