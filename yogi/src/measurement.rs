use bson::{self, Bson};
use clap::ArgMatches;
use mongodb::db::{Database, ThreadedDatabase};
use proddle::{self, Measurement, Parameter, ProddleError};
use time;

use std::fs::File;
use std::io::Read;

pub fn add(db: &Database, matches: &ArgMatches) -> Result<(), ProddleError> {
    let file = try!(value_t!(matches, "FILE", String));
    let measurement_name = try!(value_t!(matches, "MEASUREMENT_NAME", String));
    let parameters: Option<Vec<Parameter>> = match matches.values_of("PARAMETER") {
        Some(parameters) => {
            let mut params = Vec::new();
            for parameter in parameters {
                let mut split_values = parameter.split("|");
                let name = try!(split_values.nth(0).ok_or("failed to parse parameter name")).to_owned();
                let value = try!(split_values.nth(0).ok_or("failed to parse parameter value")).to_owned();

                params.push(
                    Parameter {
                        name: name,
                        value: value,
                    }
                );
            }

            Some(params)
        },
        None => None,
    };

    let dependencies: Option<Vec<String>> = match matches.values_of("DEPENDENCY") {
        Some(dependencies) => Some(dependencies.map(|x| x.to_owned()).collect()),
        None => None,
    };

    let version = match proddle::find_measurement(db, &measurement_name, None, true) {
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
    let timestamp = Some(time::now_utc().to_timespec().sec);
    let measurement = Measurement {
        timestamp: timestamp,
        name: measurement_name,
        version: version,
        parameters: parameters,
        dependencies: dependencies,
        content: Some(buffer),
    };

    //insert document
    if let Bson::Document(document) = try!(bson::to_bson(&measurement)) {
        try!(db.collection("measurements").insert_one(document, None));
    } else {
        return Err(ProddleError::from("failed to parse Measurement into OrdererdDocument"));
    }

    Ok(())
}

pub fn delete(_: &Database, _: &ArgMatches) -> Result<(), ProddleError> {
    unimplemented!();
}

pub fn search(db: &Database, matches: &ArgMatches) -> Result<(), ProddleError> {
    let measurement_name = try!(value_t!(matches, "MEASUREMENT_NAME", String));
    
    let cursor = try!(proddle::find_measurements(db, Some(&measurement_name), None, Some(1), true));
    for document in cursor {
        let document = try!(document);
        println!("{:?}", document);
    }

    Ok(())
}
