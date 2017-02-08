use bson::Bson;
use clap::ArgMatches;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use proddle;
use proddle::Error;
use time;

pub fn add(client: Client, matches: &ArgMatches) -> Result<(), Error> {
    let measurement_name = try!(value_t!(matches, "MEASUREMENT_NAME", String));
    let domain = try!(value_t!(matches, "DOMAIN", String));
    let url = try!(value_t!(matches, "URL", String));
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
    try!(proddle::find_measurement(client.clone(), &measurement_name, None, true));

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
    try!(client.db("proddle").collection("operations").insert_one(document, None));
    Ok(())
}

pub fn delete(_: Client, _: &ArgMatches) -> Result<(), Error> {
    unimplemented!();
}

pub fn search(client: Client, matches: &ArgMatches) -> Result<(), Error> {
    let domain = try!(value_t!(matches, "DOMAIN", String));

    let cursor = try!(proddle::find_operations(client.clone(), Some(&domain), None, None, true));
    for document in cursor {
        println!("{:?}", document);
    }

    Ok(())
}
