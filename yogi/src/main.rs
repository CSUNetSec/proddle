extern crate bson;
#[macro_use]
extern crate clap;
extern crate mongodb;
extern crate proddle;
extern crate rand;
extern crate time;

use clap::{App, ArgMatches};
use mongodb::{Client, ClientOptions, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use proddle::ProddleError;

mod operation;

fn parse_args(matches: &ArgMatches) -> Result<(String, u16, String, String, String, String, String), ProddleError> {
    let mongodb_ip_address = try!(value_t!(matches, "MONGODB_IP_ADDRESS", String));
    let mongodb_port = try!(value_t!(matches.value_of("MONGODB_PORT"), u16));
    let ca_file = try!(value_t!(matches.value_of("CA_FILE"), String));
    let certificate_file = try!(value_t!(matches.value_of("CERTIFICATE_FILE"), String));
    let key_file = try!(value_t!(matches.value_of("KEY_FILE"), String));
    let username = try!(value_t!(matches.value_of("USERNAME"), String));
    let password = try!(value_t!(matches.value_of("PASSWORD"), String));

    Ok((mongodb_ip_address, mongodb_port, ca_file, certificate_file, key_file, username, password))
}

fn main() {
    let yaml = load_yaml!("args.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    //initialize bridge parameters
    let (mongodb_ip_address, mongodb_port, ca_file, certificate_file, key_file, username, password) = match parse_args(&matches) {
        Ok(args) => args,
        Err(e) => panic!("{}", e),
    };

    //connect to mongodb
    let connect_result = if ca_file.eq("") && certificate_file.eq("") && key_file.eq("") {
        Client::connect(&mongodb_ip_address, mongodb_port)
    } else {
        let client_options = ClientOptions::with_ssl(&ca_file, &certificate_file, &key_file, true);
        Client::connect_with_options(&mongodb_ip_address, mongodb_port, client_options)
    };

    let client = match connect_result {
        Ok(client) => client,
        Err(e) => panic!("failed to connect to mongodb: {}", e),
    };

    let db = client.db("proddle");
    if let Err(e) = db.auth(&username, &password) {
        panic!("{}", e);
    }

    let result = if let Some(matches) = matches.subcommand_matches("operation") {
        if let Some(matches) = matches.subcommand_matches("add") {
            operation::add(&db, matches)
        } else if let Some(matches) = matches.subcommand_matches("delete") {
            operation::delete(&db, matches)
        } else if let Some(matches) = matches.subcommand_matches("search") {
            operation::search(&db, matches)
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
