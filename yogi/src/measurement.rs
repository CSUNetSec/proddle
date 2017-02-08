use bson::Bson;
use clap::ArgMatches;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use proddle;
use proddle::Error;
use time;

use std::fs::File;
use std::io::Read;

pub fn add(client: Client, matches: &ArgMatches) -> Result<(), Error> {
    let file = try!(value_t!(matches, "FILE", String));
    let measurement_name = try!(value_t!(matches, "MEASUREMENT_NAME", String));
    let parameters: Vec<Bson> = match matches.values_of("PARAMETER") {
        Some(parameters) => {
            let mut params = Vec::new();
            for parameter in parameters {
                let mut split_values = parameter.split("|");
                let name = try!(split_values.nth(0).ok_or("failed to parse parameter name")).to_owned();
                let value = try!(split_values.nth(0).ok_or("failed to parse parameter value")).to_owned();
                params.push(Bson::Document(doc! {"name" => name, "value" => value}));
            }

            params
        },
        None => Vec::new(),
    };

    let dependencies: Vec<Bson> = match matches.values_of("DEPENDENCY") {
        Some(dependencies) => dependencies.map(|x| Bson::String(x.to_owned())).collect(),
        None => Vec::new(),
    };

    let version = match proddle::find_measurement(client.clone(), &measurement_name, None, true) {
        Ok(Some(document)) => {
            match document.get("version") {
                Some(&Bson::I32(document_version)) => document_version + 1,
                _ => panic!("failed to parse version as i32"),
            }
        },
        _ => 1,
    };

    //read file into string buffer
    let mut file = try!(File::open(file));
    let mut buffer = String::new();
    try!(file.read_to_string(&mut buffer));

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
    try!(client.db("proddle").collection("measurements").insert_one(document, None));
    Ok(())
}

pub fn delete(_: Client, _: &ArgMatches) -> Result<(), Error> {
    unimplemented!();
}

pub fn search(client: Client, matches: &ArgMatches) -> Result<(), Error> {
    let measurement_name = try!(value_t!(matches, "MEASUREMENT_NAME", String));
    
    let cursor = try!(proddle::find_measurements(client.clone(), Some(&measurement_name), None, Some(1), true));
    for document in cursor {
        let document = try!(document);
        println!("{:?}", document);
    }

    Ok(())
}
