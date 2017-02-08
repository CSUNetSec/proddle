#[macro_use(bson, doc)]
extern crate bson;
#[macro_use]
extern crate clap;
extern crate mongodb;
extern crate proddle;
extern crate rand;
extern crate rustc_serialize;
extern crate time;

use bson::Bson;
use clap::{App, ArgMatches};
use mongodb::{Client, ClientOptions, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use proddle::Error;

mod measurement;

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

    if let Some(matches) = matches.subcommand_matches("measurement") {
        if let Some(matches) = matches.subcommand_matches("add") {
            if let Err(e) = measurement::add(client, matches) {
                panic!("{}", e);
            }
        } else if let Some(matches) = matches.subcommand_matches("delete") {
            if let Err(e) = measurement::delete(client, matches) {
                panic!("{}", e);
            }
        } else if let Some(matches) = matches.subcommand_matches("search") {
            if let Err(e) = measurement::search(client, matches) {
                panic!("{}", e);
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("operation") {
        if let Some(matches) = matches.subcommand_matches("add") {
            let measurement_name = matches.value_of("MEASUREMENT_NAME").unwrap();
            let domain = matches.value_of("DOMAIN").unwrap();
            let url = matches.value_of("URL").unwrap();
            let parameters: Vec<Bson> = match matches.values_of("PARAMETER") {
                Some(parameters) => {
                    parameters.map(
                            |x| {
                                let mut split_values = x.split("|");
                                let name = match split_values.nth(0) {
                                    Some(name) => name.to_owned(),
                                    None => panic!("failed to parse name of parameter"),
                                };

                                let value = match split_values.nth(0) {
                                    Some(value) => value.to_owned(),
                                    None => panic!("failed to parse value of parameter '{}'", name),
                                };

                                Bson::Document(doc! {"name" => name, "value" => value})
                            }
                        ).collect()
                },
                None => Vec::new(),
            };

            let tags: Vec<Bson> = match matches.values_of("TAG") {
                Some(tags) => tags.map(|x| Bson::String(x.to_owned())).collect(),
                None => Vec::new(),
            };

            //check if measurement exists
            match proddle::find_measurement(client.clone(), measurement_name, None, true) {
                Ok(Some(_)) => {},
                _ => panic!("measurement does not exist"),
            }

            //create opeation document
            let timestamp = time::now_utc().to_timespec().sec;
            let document = doc! {
                "timestamp" => timestamp,
                "measurement" => measurement_name,
                "domain" => domain,
                "url" => url,
                "parameters" => parameters,
                "tags" => tags
            };

            //insert document
            if let Err(e) = client.db("proddle").collection("operations").insert_one(document, None) {
                panic!("failed to upload operations document: {}", e);
            }
        } else if let Some(matches) = matches.subcommand_matches("delete") {
            unimplemented!();
        } else if let Some(matches) = matches.subcommand_matches("search") {
            let domain = matches.value_of("DOMAIN").unwrap();

            match proddle::find_operations(client.clone(), Some(domain), None, None, true) {
                Ok(cursor) => {
                    for document in cursor {
                        println!("{:?}", document);
                    }
                },
                Err(e) => panic!("failed to find operations: {}", e),
            }
        }
    }
}
