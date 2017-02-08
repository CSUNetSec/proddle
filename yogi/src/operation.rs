use bson::Bson;
use clap::ArgMatches;
use mongodb::db::{Database, ThreadedDatabase};
use proddle;
use proddle::Error;
use time;

pub fn add(db: &Database, matches: &ArgMatches) -> Result<(), Error> {
    let measurement_name = try!(value_t!(matches, "MEASUREMENT_NAME", String));
    let domain = try!(value_t!(matches, "DOMAIN", String));
    let url = try!(value_t!(matches, "URL", String));
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

    let tags: Vec<Bson> = match matches.values_of("TAG") {
        Some(tags) => tags.map(|x| Bson::String(x.to_owned())).collect(),
        None => Vec::new(),
    };

    //check if measurement exists
    try!(proddle::find_measurement(db, &measurement_name, None, true));

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
    try!(db.collection("operations").insert_one(document, None));
    Ok(())
}

pub fn delete(_: &Database, _: &ArgMatches) -> Result<(), Error> {
    unimplemented!();
}

pub fn search(db: &Database, matches: &ArgMatches) -> Result<(), Error> {
    let domain = try!(value_t!(matches, "DOMAIN", String));

    let cursor = try!(proddle::find_operations(db, Some(&domain), None, None, true));
    for document in cursor {
        println!("{:?}", document);
    }

    Ok(())
}
