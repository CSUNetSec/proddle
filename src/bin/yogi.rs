#[macro_use(bson, doc)]
extern crate bson;
extern crate bzip2;
extern crate docopt;
extern crate mongodb;
extern crate rand;
extern crate rustc_serialize;
extern crate time;

use bson::Bson;
use bson::spec::BinarySubtype;
use bzip2::Compression;
use bzip2::read::{BzDecoder, BzEncoder};
use docopt::Docopt;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{CursorType, FindOptions};

use std::fs::File;
use std::io::{Cursor, Read, Write};

const USAGE: &'static str = "
yogi

USAGE:
    yogi (-h | --help)
    yogi cancel <module-name> <domain>
    yogi download <local-filename> <module-name> [--version=<version>]
    yogi schedule <module-name> <domain>
    yogi search (--module-name=<module-name> | --domain=<domain>)
    yogi upload <local-filename> <module-name> [<dependency>...]

OPTIONS:
    -h --help                       Display this screen.
    --domain=<domain>               Domain to perform operation.
    --module-name=<module-name>     Name of module to perform operation.
    --version=<version>             Specify version to perform operation.
";

#[derive(Debug, RustcDecodable)]
struct Args {
    cmd_cancel: bool,
    cmd_download: bool,
    cmd_schedule: bool,
    cmd_search: bool,
    cmd_upload: bool,
    arg_dependency: Vec<String>,
    arg_domain: String,
    arg_local_filename: String,
    arg_module_name: String,
    flag_domain: Option<String>,
    flag_module_name: Option<String>,
    flag_version: Option<i32>,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                        .and_then(|d| d.decode())
                        .unwrap_or_else(|e| e.exit());

    let client = Client::connect("localhost", 27017).ok().expect("failed to initialize connection with mongodb");

    if args.cmd_cancel {
        unimplemented!();
    } else if args.cmd_download {
        //specify modules collection
        let collection = client.db("proddle").collection("modules");

        //query db for correct document
        let module_name = args.arg_module_name;
        let document = match args.flag_version {
            Some(search_version) => {
                let search_document = doc! {
                    "module_name" => module_name,
                    "version" => search_version
                };

                collection.find_one(Some(search_document), None)
            },
            None => {
                let search_document = doc! {
                    "module_name" => module_name
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
            }
        };

        //handle document result
        let definition = match document {
            Ok(Some(document)) => {
                //retrieve definition of module
                match document.get("definition") {
                    Some(&Bson::Binary(BinarySubtype::Generic, ref definition)) => definition.to_owned(),
                    _ => panic!("could not parse 'definiton' as binary field"),
                }
            },
            _ => panic!("could not find document"),
        };

        //decompress definition
        let mut bz_decoder = BzDecoder::new(Cursor::new(definition));

        let mut buffer = Vec::new();
        if let Err(e) = bz_decoder.read_to_end(&mut buffer) {
            panic!("failed to decompress file: {}", e);
        }

        //write to file
        let mut file = match File::create(args.arg_local_filename) {
            Ok(file) => file,
            Err(e) => panic!("failed to create local file: {}", e),
        };

        if let Err(e) = file.write_all(&buffer) {
            panic!("failed to write to local file: {}", e);
        }
    } else if args.cmd_schedule {
        unimplemented!();
    } else if args.cmd_search {
        if let Some(_) = args.flag_domain {
            unimplemented!();
        } else if let Some(module_name) = args.flag_module_name {
            //specify collection to query
            let collection = client.db("proddle").collection("modules");

            //create module document
            let document = doc! {
                "module_name" => module_name
            };

            //fetch cursor
            let cursor = match collection.find(Some(document), None) {
                Ok(cursor) => cursor,
                Err(e) => panic!("failed to fetch cursor: {}", e),
            };

            for item in cursor {
                match item {
                    Ok(item) => println!("{:?}", item),
                    Err(e) => panic!("failed to retrieve item: {}", e),
                }
            }
        }
    } else if args.cmd_upload {
        //specify modules collection
        let collection = client.db("proddle").collection("modules");

        //retrieve correct version
        let module_name = &args.arg_module_name;
        let search_document = doc! {
            "module_name" => module_name
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

        let version = match collection.find_one(Some(search_document), Some(find_options)) {
            Ok(Some(document)) => {
                match document.get("version") {
                    Some(&Bson::I32(document_version)) => document_version + 1,
                    _ => panic!("failed to parse version as i32"),
                }
            },
            _ => 1,
        };

        //read file into compressed binary buffer
        let file = match File::open(args.arg_local_filename) {
            Ok(file) => file,
            Err(e) => panic!("failed to open local file : {}", e),
        };

        let mut bz_encoder = BzEncoder::new(file, Compression::Best);

        let mut buffer = Vec::new();
        if let Err(e) = bz_encoder.read_to_end(&mut buffer) {
            panic!("failed to read local file: {}", e);
        }

        //create module document
        let timestamp = time::now_utc().to_timespec().sec;
        let dependencies: Vec<Bson> = args.arg_dependency.iter().map(|x| Bson::String(x.to_owned())).collect();
        let definition = Bson::Binary(BinarySubtype::Generic, buffer);
        let document = doc! { 
            "timestamp" => timestamp,
            "module_name" => module_name,
            "version" => version,
            "dependencies" => dependencies,
            "definition" => definition
        };

        //insert document
        if let Err(e) = collection.insert_one(document, None) {
            panic!("failed to upload module document: {}", e);
        }
    }
}
