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
use bzip2::read::BzEncoder;
use docopt::Docopt;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;

use std::fs::File;
use std::io::Read;

const USAGE: &'static str = "
yogi

USAGE:
    yogi (-h | --help)
    yogi cancel <module-name> <domain>
    yogi download <local-filename> <module-name>
    yogi schedule <module-name> <domain>
    yogi search (--module-name=<module-name> | --domain=<domain>)
    yogi upload <local-filename> <module-name> [<dependency>...]

OPTIONS:
    -h --help                       Display this screen.
    --domain=<domain>               Domain to perform operation.
    --module-name=<module-name>     Name of module to perform operation.
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
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                        .and_then(|d| d.decode())
                        .unwrap_or_else(|e| e.exit());

    let client = Client::connect("localhost", 27017).ok().expect("failed to initialize connection with mongodb");

    if args.cmd_cancel {
        unimplemented!();
    } else if args.cmd_download {
        unimplemented!();
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
                Err(e) => panic!("unable to fetch cursor: {}", e),
            };

            for item in cursor {
                match item {
                    Ok(item) => println!("{:?}", item),
                    Err(e) => panic!("failed to retrieve item: {}", e),
                }
            }
        }
    } else if args.cmd_upload {
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

        //specify modules collection
        let collection = client.db("proddle").collection("modules");

        //create module document
        let module_id = rand::random::<u64>();
        let timestamp = time::now_utc().to_timespec().sec;
        let module_name = args.arg_module_name;
        let dependencies: Vec<Bson> = args.arg_dependency.iter().map(|x| Bson::String(x.to_owned())).collect();
        let definition = Bson::Binary(BinarySubtype::Generic, buffer);
        let document = doc! { 
            "_id" => module_id,
            "timestamp" => timestamp,
            "module_name" => module_name,
            "dependencies" => dependencies,
            "definition" => definition
        };

        //insert document
        if let Err(e) = collection.insert_one(document, None) {
            panic!("failed to upload module document: {}", e);
        }
    }
}
