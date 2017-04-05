use bson::{self, Bson};
use clap::ArgMatches;
use mongodb::db::{Database, ThreadedDatabase};
use proddle::{Operation, Parameter, ProddleError};
use time;

pub fn add(db: &Database, matches: &ArgMatches) -> Result<(), ProddleError> {
    let measurement_class = try!(value_t!(matches, "MEASUREMENT_CLASS", String));
    let domain = try!(value_t!(matches, "DOMAIN", String));
    let parameters: Vec<Parameter> = match matches.values_of("PARAMETER") {
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

            params
        },
        None => Vec::new(),
    };

    let tags: Vec<String> = match matches.values_of("TAG") {
        Some(tags) => tags.map(|x| x.to_owned()).collect(),
        None => Vec::new(),
    };

    //create opeation document
    let timestamp = time::now_utc().to_timespec().sec;
    let operation = Operation {
        timestamp: timestamp,
        measurement_class: measurement_class,
        domain: domain,
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
