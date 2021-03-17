use jmespath::{
    functions::{ArgumentType, CustomFunction, Signature},
    Context, Expression, Rcvar, Runtime,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum TransformError {
    #[error("Unable to mutate non-object value {value}")]
    ValueNotAnObject { value: Value },

    #[error("JmespathError: {source}")]
    JmesPath {
        #[from]
        source: jmespath::JmespathError,
    },

    #[error("serde_json::Error: {source}")]
    Json {
        #[from]
        source: serde_json::Error,
    },
}

pub(crate) struct TransformContext<'a> {
    transforms: HashMap<String, Expression<'a>>,
}

impl<'a> TransformContext<'a> {
    pub(crate) fn new(
        runtime: &'a mut Runtime,
        transform_definitions: &HashMap<String, String>,
    ) -> Result<Self, TransformError> {
        runtime.register_builtin_functions();
        runtime.register_function("substr", Box::new(Self::substr()));

        let mut transforms = HashMap::new();

        for (k, v) in transform_definitions.iter() {
            let expression = runtime.compile(v.as_str())?;
            transforms.insert(k.to_owned(), expression);
        }

        Ok(Self { transforms })
    }

    pub(crate) fn transform(&self, value: &mut Value) -> Result<(), TransformError> {
        let data = jmespath::Variable::from(value.clone());

        match value.as_object_mut() {
            Some(map) => {
                for (property, expression) in self.transforms.iter() {
                    let variable = expression.search(data.clone())?;

                    let property_value = serde_json::to_value(variable)?;

                    map.insert(property.to_string(), property_value);
                }
                Ok(())
            }
            _ => Err(TransformError::ValueNotAnObject {
                value: value.to_owned(),
            }),
        }
    }

    fn substr() -> CustomFunction {
        CustomFunction::new(
            Signature::new(
                vec![
                    ArgumentType::String,
                    ArgumentType::String,
                    ArgumentType::String,
                ],
                None,
            ),
            Box::new(|args: &[Rcvar], context: &mut Context| {
                let s = args[0].as_string().unwrap();

                let start = args[1].as_string().unwrap().parse::<usize>().unwrap();
                let end = args[2].as_string().unwrap().parse::<usize>().unwrap();

                let s2: String = s.chars().skip(start).take(end).collect();

                let sv: Value = serde_json::Value::String(s2);

                let var = jmespath::Variable::from(sv);

                Ok(Arc::new(var))
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use rdkafka::message::OwnedMessage;

    use super::*;
    use serde_json::json;

    #[test]
    fn transform() {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error")).init();

        let mut test_value = json!({
            "name": "A",
            "modified": "2021-03-16T14:38:58Z",
        });

        let test_message = OwnedMessage::new(
            Some(test_value.to_string().into_bytes()),
            None,
            "test".to_string(),
            rdkafka::Timestamp::NotAvailable,
            0,
            0,
            None,
        );

        let mut transforms = HashMap::new();

        transforms.insert(
            "modified_date".to_string(),
            "substr(modified, '0', '10')".to_string(),
        );

        let mut runtime = Runtime::new();
        let transformer = TransformContext::new(&mut runtime, &transforms).unwrap();

        let _ = transformer.transform(&mut test_value).unwrap();

        let name = test_value.get("name").unwrap().as_str().unwrap();
        let modified = test_value.get("modified").unwrap().as_str().unwrap();
        let modified_date = test_value.get("modified_date").unwrap().as_str().unwrap();

        assert_eq!("A", name);
        assert_eq!("2021-03-16T14:38:58Z", modified);
        assert_eq!("2021-03-16", modified_date);
    }
}
