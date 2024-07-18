use jmespatch::{Expression, Runtime};
use serde_json::Value;

use crate::filters::filter::Filter;
use crate::filters::jmespath_filter::custom_functions::create_eq_ignore_case_function;
use crate::filters::FilterError;

lazy_static! {
    static ref FILTER_RUNTIME: Runtime = {
        let mut runtime = Runtime::new();
        runtime.register_builtin_functions();
        runtime.register_function("eq_ignore_case", Box::new(create_eq_ignore_case_function()));
        runtime
    };
}

/// Implementation of the [Filter] trait for complex checks, such as checking for
/// the presence of a key in an object or comparing the second value in an array
/// or check array length.
/// More examples: https://jmespath.org/examples.html; https://jmespath.org/tutorial.html
pub struct JmespathFilter {
    filters: Vec<Expression<'static>>,
}

impl Filter for JmespathFilter {
    fn from_filters(filters: &[String]) -> Result<Self, FilterError> {
        let filters = filters
            .iter()
            .map(|f| {
                FILTER_RUNTIME
                    .compile(f)
                    .map_err(|source| FilterError::CompileExpressionError { source })
            })
            .collect::<Result<Vec<Expression<'static>>, FilterError>>();
        match filters {
            Ok(filters) => Ok(Self { filters }),
            Err(e) => Err(e),
        }
    }

    fn filter(&self, message: &Value) -> Result<(), FilterError> {
        if self.filters.is_empty() {
            return Ok(());
        }

        for filter in &self.filters {
            match filter.search(message) {
                Err(e) => return Err(FilterError::JmespathError { source: e }),
                Ok(v) => {
                    if !v.as_boolean().unwrap() {
                        return Err(FilterError::FilterSkipMessage);
                    }
                }
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io;
    use std::io::{BufRead, BufReader};

    use super::*;

    const SOURCE_PATH: &str = "tests/json/web_requests-100.json";

    fn read_json_file(file_path: &str) -> io::Result<Vec<Value>> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let lines: Vec<_> = reader.lines().take(30000).collect::<io::Result<_>>()?;

        let values: Vec<Value> = lines
            .iter()
            .map(|line| serde_json::from_str::<Value>(&line).unwrap())
            .collect();

        Ok(values)
    }

    fn run_filter(filter: &JmespathFilter, values: &Vec<Value>) -> (i32, i32) {
        let mut passed_messages = 0;
        let mut filtered_messages = 0;

        for v in values.into_iter() {
            match filter.filter(&v) {
                Ok(_) => passed_messages += 1,
                Err(FilterError::FilterSkipMessage) => filtered_messages += 1,
                Err(e) => panic!("{}", e),
            }
        }

        return (passed_messages, filtered_messages);
    }
    #[test]
    fn equal() {
        let values = read_json_file(SOURCE_PATH).unwrap();
        let filter = match JmespathFilter::from_filters(&vec![
            "session_id=='a8a3d0e3-7b4e-4f17-b264-76cb792bdb96'".to_string(),
        ]) {
            Ok(f) => f,
            Err(e) => panic!("{}", e),
        };

        let (passed_messages, filtered_messages) = run_filter(&filter, &values);

        assert_eq!(1, passed_messages);
        assert_eq!(99, filtered_messages);
    }
    #[test]
    fn eq_ignore_case() {
        let values = read_json_file(SOURCE_PATH).unwrap();
        let filter = match JmespathFilter::from_filters(&vec![
            "eq_ignore_case(method, 'get')".to_string()
        ]) {
            Ok(f) => f,
            Err(e) => panic!("{}", e),
        };

        let (passed_messages, filtered_messages) = run_filter(&filter, &values);

        assert_eq!(17, passed_messages);
        assert_eq!(83, filtered_messages);
    }

    #[test]
    fn or_condition() {
        let values = read_json_file(SOURCE_PATH).unwrap();
        let filter = match JmespathFilter::from_filters(&vec![
            "(status == `404` || method == 'GET')".to_string(),
        ]) {
            Ok(f) => f,
            Err(e) => panic!("{}", e),
        };

        let (passed_messages, filtered_messages) = run_filter(&filter, &values);

        assert_eq!(25, passed_messages);
        assert_eq!(75, filtered_messages);
    }

    #[test]
    fn complex_condition() {
        let buff = r#"{"name": "John Doe", "age": 30, "status": "1", "department": "Engineering"}
            {"name": "Jane Smith", "age": 25, "status": "1", "department": "Marketing"}
            {"name": "Emily Johnson", "age": 35, "department": "Sales"}
            {"name": "Michael Brown", "age": 40, "status": "3", "department": "Engineering"}
            {"name": "Sarah Davis", "age": 28, "department": "Marketing"}
            {"name": "David Wilson", "age": 22, "department": "Sales"}
            {"name": "Laura Martinez", "age": 33, "status": "2", "department": "Engineering"}
            {"name": "James Anderson", "age": 45, "department": "Marketing"}
            {"name": "Linda Thomas", "age": 50, "department": "Sales"}
            {"name": "Robert Jackson", "age": 37, "department": "Engineering"}"#;

        let objects = buff.split("\n").map(|s| s.trim()).collect::<Vec<&str>>();
        let values: Vec<Value> = objects
            .iter()
            .map(|line| serde_json::from_str::<Value>(&line).unwrap())
            .collect();
        let filter = match JmespathFilter::from_filters(&vec![
            "!contains(keys(@), 'status') || (status == '1' && age >= `26`)".to_string(),
        ]) {
            Ok(f) => f,
            Err(e) => panic!("{}", e),
        };

        let (passed_messages, filtered_messages) = run_filter(&filter, &values);

        assert_eq!(7, passed_messages);
        assert_eq!(3, filtered_messages);
    }
}
