use error::ProddleError;
use parameter::Parameter;
use proddle_capnp;

#[derive(Deserialize, Serialize)]
pub struct Measurement {
    pub timestamp: Option<u64>,
    pub name: String,
    pub version: u16,
    pub parameters: Option<Vec<Parameter>>,
    pub dependencies: Option<Vec<String>>,
    pub content: Option<String>,
}

impl Measurement {
    pub fn new(timestamp: Option<u64>, name: String, version: u16, parameters: Option<Vec<Parameter>>, dependencies: Option<Vec<String>>, content: Option<String>) -> Measurement {
        Measurement {
            timestamp: timestamp,
            name: name,
            version: version,
            parameters: parameters,
            dependencies: dependencies,
            content: content,
        }
    }

    pub fn from_capnproto(msg: &proddle_capnp::measurement::Reader) -> Result<Measurement, ProddleError> {
        let timestamp = match msg.get_timestamp() {
            0 => None,
            _ => Some(msg.get_timestamp()),
        };

        let measurement_name = msg.get_name().unwrap().to_owned();
        let version = msg.get_version();

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

        let dependencies = match msg.has_dependencies() {
            true => Some(msg.get_dependencies().unwrap().iter().map(|x| x.unwrap().to_string()).collect()),
            false => None,
        };

        let content = match msg.has_content() {
            true => Some(msg.get_content().unwrap().to_owned()),
            false => None,
        };

        Ok(
            Measurement {
                timestamp: timestamp,
                name: measurement_name,
                version: version,
                parameters: parameters,
                dependencies: dependencies,
                content: content,
            }
        )
    }
}
