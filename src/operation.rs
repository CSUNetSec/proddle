use error::ProddleError;
use parameter::Parameter;
use proddle_capnp;

use std::hash::{Hash, Hasher};

#[derive(Clone, Deserialize, Serialize)]
pub struct Operation {
    pub timestamp: Option<u64>,
    pub measurement: String,
    pub domain: String,
    pub url: String,
    pub parameters: Option<Vec<Parameter>>,
    pub tags: Option<Vec<String>>,
}

impl Operation {
    pub fn from_capnproto(msg: &proddle_capnp::operation::Reader) -> Result<Operation, ProddleError> {
        let timestamp = match msg.get_timestamp() {
            0 => None,
            _ => Some(msg.get_timestamp()),
        };

        let measurement = msg.get_measurement().unwrap().to_owned();
        let domain = msg.get_domain().unwrap().to_owned();
        let url = msg.get_url().unwrap().to_owned();
        let parameters = match msg.has_parameters() {
            true => {
                let mut parameters = Vec::new();
                for parameter in msg.get_parameters().unwrap().iter() {
                    let name = match parameter.get_name() {
                        Ok(name) => name,
                        Err(_) => return Err(ProddleError::Proddle(String::from("failed to retrieve name from parameter"))),
                    };

                    let value = match parameter.get_value() {
                        Ok(value) => value,
                        Err(_) => return Err(ProddleError::Proddle(String::from("failed to retrieve value from parameter"))),
                    };

                    parameters.push(
                        Parameter {
                            name: name.to_owned(),
                            value: value.to_owned(),
                        }
                    );
                }

                Some(parameters)
            },
            false  => None,
        };

        let tags = match msg.has_tags() {
            true => Some(msg.get_tags().unwrap().iter().map(|x| x.unwrap().to_string()).collect()),
            false => None,
        };

        Ok(
            Operation {
                timestamp: timestamp,
                measurement: measurement,
                domain: domain,
                url: url,
                parameters: parameters,
                tags: tags,
            }
        )
    }
}

impl Hash for Operation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.measurement.hash(state);
        self.domain.hash(state);
        self.url.hash(state);

        if let Some(ref parameters) = self.parameters {
            for parameter in parameters {
                parameter.name.hash(state);
                parameter.value.hash(state);
            }
        }

        if let Some(ref tags) = self.tags {
            for tag in tags {
                tag.hash(state);
            }
        }
    }
}
