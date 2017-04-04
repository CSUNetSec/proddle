use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Operation {
    pub timestamp: i64,
    pub measurement_class: String,
    pub domain: String,
    pub parameters: Vec<Parameter>,
    pub tags: Vec<String>,
}

impl Hash for Operation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.measurement_class.hash(state);
        self.domain.hash(state);

        for parameter in self.parameters.iter() {
            parameter.name.hash(state);
            parameter.value.hash(state);
        }

        for tag in self.tags.iter() {
            tag.hash(state);
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Parameter {
    pub name: String,
    pub value: String,
}
