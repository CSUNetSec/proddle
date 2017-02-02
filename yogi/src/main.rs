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
use clap::App;
use mongodb::ThreadedClient;
use mongodb::db::ThreadedDatabase;

use std::fs::File;
use std::io::Read;

fn main() {
    let yaml = load_yaml!("args.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    //connect to mongodb
    let client = match proddle::get_mongodb_client("localhost", 27017) {
        Ok(client) => client,
        Err(e) => panic!("{}", e),
    };

    if let Some(matches) = matches.subcommand_matches("measurement") {
        if let Some(matches) = matches.subcommand_matches("add") {
            let file = matches.value_of("FILE").unwrap();
            let measurement_name = matches.value_of("MEASUREMENT_NAME").unwrap();
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

            let dependencies: Vec<Bson> = match matches.values_of("DEPENDENCY") {
                Some(dependencies) => dependencies.map(|x| Bson::String(x.to_owned())).collect(),
                None => Vec::new(),
            };

            let version = match proddle::find_measurement(client.clone(), measurement_name, None, true) {
                Ok(Some(document)) => {
                    match document.get("version") {
                        Some(&Bson::I32(document_version)) => document_version + 1,
                        _ => panic!("failed to parse version as i32"),
                    }
                },
                _ => 1,
            };

            //read file into string buffer
            let mut file = match File::open(file) {
                Ok(file) => file,
                Err(e) => panic!("failed to open file : {}", e),
            };

            let mut buffer = String::new();
            if let Err(e) = file.read_to_string(&mut buffer) {
                panic!("failed to read local file: {}", e);
            }

            //create measurement document
            let timestamp = time::now_utc().to_timespec().sec;
            let document = doc! { 
                "timestamp" => timestamp,
                "name" => measurement_name,
                "version" => version,
                "parameters" => parameters,
                "dependencies" => dependencies,
                "content" => buffer
            };

            //insert document
            if let Err(e) = client.db("proddle").collection("measurements").insert_one(document, None) {
                panic!("failed to upload measurement document: {}", e);
            }
        } else if let Some(matches) = matches.subcommand_matches("delete") {
            unimplemented!();
        } else if let Some(matches) = matches.subcommand_matches("search") {
            let measurement_name = matches.value_of("MEASUREMENT_NAME").unwrap();

            match proddle::find_measurements(client.clone(), Some(measurement_name), None, Some(1), true) {
                Ok(cursor) => {
                    for document in cursor {
                        let document = match document {
                            Ok(document) => document,
                            Err(e) => panic!("failed to retrieve document: {}", e),
                        };

                        println!("{:?}", document);
                    }
                },
                Err(e) => panic!("failed to find operations: {}", e),
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
