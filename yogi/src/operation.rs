use bson::{self, Bson};
use clap::ArgMatches;
use mongodb::db::{Database, ThreadedDatabase};
use proddle::{self, Operation, Parameter, ProddleError};
use time;

pub fn add(db: &Database, matches: &ArgMatches) -> Result<(), ProddleError> {
    let measurement_name = try!(value_t!(matches, "MEASUREMENT_NAME", String));
    let domain = try!(value_t!(matches, "DOMAIN", String));
    let url = try!(value_t!(matches, "URL", String));
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

    let tags: Option<Vec<String>> = match matches.values_of("TAG") {
        Some(tags) => Some(tags.map(|x| x.to_owned()).collect()),
        None => None,
    };

    //check if measurement exists
    try!(proddle::find_measurement(db, &measurement_name, None, true));

    //create opeation document
    let timestamp = Some(time::now_utc().to_timespec().sec);
    let operation = Operation {
        timestamp: timestamp,
        measurement: measurement_name,
        domain: domain,
        url: url,
        parameters: parameters,
        tags: tags,
    };

    //insert document
    if let Bson::Document(document) = try!(bson::to_bson(&operation)) {
        try!(db.collection("operations").insert_one(document, None));
    } else {
        return Err(ProddleError::from("failed to parse Operation into OrdererdDocument"));
    }

    Ok(())
}

pub fn delete(_: &Database, _: &ArgMatches) -> Result<(), ProddleError> {
    unimplemented!();
}

pub fn search(db: &Database, matches: &ArgMatches) -> Result<(), ProddleError> {
    let domain = try!(value_t!(matches, "DOMAIN", String));

    let cursor = try!(proddle::find_operations(db, Some(&domain), None, None, true));
    for document in cursor {
        println!("{:?}", document);
    }

    Ok(())
}
