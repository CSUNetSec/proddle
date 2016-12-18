#[macro_use(bson, doc)]
extern crate bson;
extern crate bzip2;
#[macro_use]
extern crate clap;
extern crate mongodb;
extern crate rand;
extern crate rustc_serialize;
extern crate time;

use bson::Bson;
use bson::ordered::OrderedDocument;
use bson::spec::BinarySubtype;
use bzip2::Compression;
use bzip2::read::{BzDecoder, BzEncoder};
use mongodb::{Client, ClientInner, ThreadedClient};
use mongodb::cursor::Cursor as MongodbCursor;
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{CursorType, FindOptions};

use std::fs::File;
use std::io::{Cursor, Read};
use std::sync::Arc;

fn main() {
    let matches = clap_app!(yogi =>
            (version: "1.0")
            (author: "hamersaw <hamersaw@bushpath.com>")
            (@subcommand module =>
                (about: "Perform actions on modules")
                (@subcommand add =>
                    (about: "Add a module")
                    (@arg FILE: +required "Filename of module")
                    (@arg MODULE: +required "Name of module")
                    (@arg DEPENDENCY: -d --dependency +takes_value ... "Python dependencies of the module")
                )
                (@subcommand delete =>
                    (about: "Delete a module")
                    (@arg MODULE: +required "Name of module")
                )
                (@subcommand search =>
                    (about: "Search for a module")
                    (@arg MODULE: +required "Name of module")
                )
            )
            (@subcommand operation =>
                (about: "Perform actions on operations")
                (@subcommand add =>
                    (about: "Add a operation")
                    (@arg MODULE: +required "Name of module")
                    (@arg DOMAIN: +required "Domain name")
                    (@arg INTERVAL: -i --interval +takes_value "Operation execution interval in seconds")
                )
                (@subcommand delete =>
                    (about: "delete an operation")
                    (@arg DOMAIN: +required "Domain name")
                )
                (@subcommand search =>
                    (about: "Search for an operation")
                    (@arg DOMAIN: +required "Domain name")
                )
            )
        ).get_matches();

    //connect to mongodb
    let client = Client::connect("localhost", 27017).ok().expect("failed to initialize connection with mongodb");

    if let Some(matches) = matches.subcommand_matches("module") {
        if let Some(matches) = matches.subcommand_matches("add") {
            let file = matches.value_of("FILE").unwrap();
            let module = matches.value_of("MODULE").unwrap();
            let dependencies: Vec<Bson> = match matches.values_of("DEPENDENCY") {
                Some(dependencies) => dependencies.map(|x| Bson::String(x.to_owned())).collect(),
                None => Vec::new(),
            };

            let version = match find_module(client.clone(), module, None) {
                Ok(Some(document)) => {
                    match document.get("version") {
                        Some(&Bson::I32(document_version)) => document_version + 1,
                        _ => panic!("failed to parse version as i32"),
                    }
                },
                _ => 1,
            };

            //read file into compressed binary buffer
            let file = match File::open(file) {
                Ok(file) => file,
                Err(e) => panic!("failed to open file : {}", e),
            };

            let mut bz_encoder = BzEncoder::new(file, Compression::Best);

            let mut buffer = Vec::new();
            if let Err(e) = bz_encoder.read_to_end(&mut buffer) {
                panic!("failed to read local file: {}", e);
            }

            //create module document
            let timestamp = time::now_utc().to_timespec().sec;
            let content = Bson::Binary(BinarySubtype::Generic, buffer);
            let document = doc! { 
                "timestamp" => timestamp,
                "module" => module,
                "version" => version,
                "dependencies" => dependencies,
                "content" => content
            };

            //insert document
            if let Err(e) = client.db("proddle").collection("modules").insert_one(document, None) {
                panic!("failed to upload module document: {}", e);
            }
        } else if let Some(matches) = matches.subcommand_matches("delete") {
            unimplemented!();
        } else if let Some(matches) = matches.subcommand_matches("search") {
            let module = matches.value_of("MODULE").unwrap();

            //search for latest version of module
            match find_module(client.clone(), module, None) {
                Ok(Some(document)) => {
                    let timestamp = document.get("timestamp").unwrap();
                    let module = document.get("module").unwrap();
                    let version = document.get("version").unwrap();
                    let dependencies = document.get("dependencies").unwrap();
                    let content = match document.get("content") {
                        Some(&Bson::Binary(BinarySubtype::Generic, ref content)) => content.to_owned(),
                        _ => panic!("could not parse 'definiton' as binary field"),
                    };

                    //decompress content
                    let mut bz_decoder = BzDecoder::new(Cursor::new(content));

                    let mut buffer = Vec::new();
                    if let Err(e) = bz_decoder.read_to_end(&mut buffer) {
                        panic!("failed to decompress file: {}", e);
                    }

                    let content = String::from_utf8(buffer).unwrap();
                    println!("timestamp:{}\nmodule:{}\nversion:{}\ndependencies:{}\ncontent:{:?}", timestamp, module, version, dependencies, content);
                }
                _ => panic!("module not found"),
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("operation") {
        if let Some(matches) = matches.subcommand_matches("add") {
            let module = matches.value_of("MODULE").unwrap();
            let domain = matches.value_of("DOMAIN").unwrap();
            let interval = match matches.value_of("INTERVAL") {
                Some(interval) => {
                    match interval.parse::<i32>() {
                        Ok(interval) => interval,
                        Err(e) => panic!("failed to parse interval into integer: {}", e),
                    }
                },
                None => 14400,
            };

            //check if module exists
            match find_module(client.clone(), module, None) {
                Ok(Some(_)) => {},
                _ => panic!("module does not exist"),
            }

            //create opeation document
            let timestamp = time::now_utc().to_timespec().sec;
            let document = doc! {
                "timestamp" => timestamp,
                "module" => module,
                "domain" => domain,
                "interval" => interval
            };

            //insert document
            if let Err(e) = client.db("proddle").collection("operations").insert_one(document, None) {
                panic!("failed to upload operations document: {}", e);
            }
        } else if let Some(matches) = matches.subcommand_matches("delete") {
            unimplemented!();
        } else if let Some(matches) = matches.subcommand_matches("search") {
            let domain = matches.value_of("DOMAIN").unwrap();

            match find_operations(client.clone(), domain, None) {
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

fn find_module(client: Arc<ClientInner>, module: &str, version: Option<i32>) -> Result<Option<OrderedDocument>, mongodb::Error> {
    //specify modules collection
    let collection = client.db("proddle").collection("modules");

    //query db for correct document
    match version {
        Some(search_version) => {
            let search_document = doc! {
                "module" => module,
                "version" => search_version
            };

            collection.find_one(Some(search_document), None)
        },
        None => {
            let search_document = doc! {
                "module" => module
            };

            let negative_one = -1;
            let find_options = FindOptions {
                allow_partial_results: false,
                no_cursor_timeout: false,
                op_log_replay: false,
                skip: 0,
                limit: 1,
                cursor_type: CursorType::NonTailable,
                batch_size: 0,
                comment: None,
                max_time_ms: None,
                modifiers: None,
                projection: None,
                sort: Some(doc! {
                    "version" => negative_one
                }),
                read_preference: None,
            };

            collection.find_one(Some(search_document), Some(find_options))
        },
    }
}

fn find_operations(client: Arc<ClientInner>, domain: &str, module: Option<&str>) -> Result<MongodbCursor, mongodb::Error> {
    //specify operations collection
    let collection = client.db("proddle").collection("operations");

    //query db for correct documents
    match module {
        Some(search_module) => {
            let search_document = doc! {
                "domain" => domain,
                "module" => search_module
            };

            collection.find(Some(search_document), None)
        },
        None => {
            let search_document = doc! {
                "domain" => domain
            };

            let negative_one = -1;
            let find_options = FindOptions {
                allow_partial_results: false,
                no_cursor_timeout: false,
                op_log_replay: false,
                skip: 0,
                limit: 0,
                cursor_type: CursorType::NonTailable,
                batch_size: 0,
                comment: None,
                max_time_ms: None,
                modifiers: None,
                projection: None,
                sort: Some(doc! {
                    "timestamp" => negative_one
                }),
                read_preference: None,
            };

            collection.find(Some(search_document), Some(find_options))
        },
    }
}
