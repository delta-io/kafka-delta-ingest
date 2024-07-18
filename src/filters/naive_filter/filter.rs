use regex::Regex;
use serde_json::Value;

use crate::filters::filter::Filter;
use crate::filters::naive_filter::operand::NaiveFilterOperand;
use crate::filters::naive_filter::operator::{get_operator, OperatorRef};
use crate::filters::FilterError;

pub struct NaiveFilterExpression {
    left: NaiveFilterOperand,
    op: OperatorRef,
    right: NaiveFilterOperand,
}

/// Implementation of the [Filter] feature for simple comparison checks.
/// If a path was provided, it must always be present in the object.
/// Possible operations: >=, <=, ==, !=, ~=, >, <
/// ~= - case-insensitive comparison, for example: path.to.attr ~= 'VaLuE'
pub(crate) struct NaiveFilter {
    filters: Vec<NaiveFilterExpression>,
}

impl Filter for NaiveFilter {
    fn from_filters(filters: &[String]) -> Result<Self, FilterError> {
        let mut expressions: Vec<NaiveFilterExpression> = Vec::new();
        let re = Regex::new(r"(?<left>.*)(?<operator>>=|<=|==|!=|~=|>|<)(?<right>.*)").unwrap();
        for filter in filters.iter() {
            let (_, [left, op, right]) = re.captures(filter.trim()).unwrap().extract();
            expressions.push(NaiveFilterExpression {
                left: NaiveFilterOperand::from_str(left)?,
                op: get_operator(op)?,
                right: NaiveFilterOperand::from_str(right)?,
            });
        }

        Ok(NaiveFilter {
            filters: expressions,
        })
    }

    fn filter(&self, message: &Value) -> Result<(), FilterError> {
        for filter in self.filters.iter() {
            if !filter.op.execute(
                filter.left.get_value(message),
                filter.right.get_value(message),
            )? {
                return Err(FilterError::FilterSkipMessage);
            }
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

    fn run_filter(filter: &NaiveFilter, values: &Vec<Value>) -> (i32, i32) {
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
    fn greater_than_or_equal() {
        let values = read_json_file(SOURCE_PATH).unwrap();
        let filter = match NaiveFilter::from_filters(&vec![
            "status>=`201`".to_string(),
            "method=='GET'".to_string(),
        ]) {
            Ok(f) => f,
            Err(e) => panic!("{}", e),
        };
        let (passed_messages, filtered_messages) = run_filter(&filter, &values);

        assert_eq!(14, passed_messages);
        assert_eq!(86, filtered_messages);
    }

    #[test]
    fn less_than_or_equal() {
        let values = read_json_file(SOURCE_PATH).unwrap();
        let filter = match NaiveFilter::from_filters(&vec![
            "status<=`403`".to_string(),
            "method=='POST'".to_string(),
        ]) {
            Ok(f) => f,
            Err(e) => panic!("{}", e),
        };
        let (passed_messages, filtered_messages) = run_filter(&filter, &values);

        assert_eq!(12, passed_messages);
        assert_eq!(88, filtered_messages);
    }

    #[test]
    fn equal() {
        let values = read_json_file(SOURCE_PATH).unwrap();
        let filter = match NaiveFilter::from_filters(&vec![
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
    fn not_equal() {
        let values = read_json_file(SOURCE_PATH).unwrap();
        let filter = match NaiveFilter::from_filters(&vec!["method!='POST'".to_string()]) {
            Ok(f) => f,
            Err(e) => panic!("{}", e),
        };

        let (passed_messages, filtered_messages) = run_filter(&filter, &values);

        assert_eq!(81, passed_messages);
        assert_eq!(19, filtered_messages);
    }
    #[test]
    fn eq_ignore_case() {
        let values = read_json_file(SOURCE_PATH).unwrap();
        let filter = match NaiveFilter::from_filters(&vec!["method~='get'".to_string()]) {
            Ok(f) => f,
            Err(e) => panic!("{}", e),
        };

        let (passed_messages, filtered_messages) = run_filter(&filter, &values);

        assert_eq!(17, passed_messages);
        assert_eq!(83, filtered_messages);
    }

    #[test]
    fn invalid_filters() {
        assert!(
            NaiveFilter::from_filters(&vec!["method~='get]".to_string()]).is_err(),
            "The filter should not have been created"
        );
        assert!(
            NaiveFilter::from_filters(&vec!["method~='get']".to_string()]).is_err(),
            "The filter should not have been created"
        );
        assert!(
            NaiveFilter::from_filters(&vec!["status~=`404".to_string()]).is_err(),
            "The filter should not have been created"
        );
        assert!(
            NaiveFilter::from_filters(&vec!["status~=`404,123`".to_string()]).is_err(),
            "The filter should not have been created"
        );
        assert!(
            NaiveFilter::from_filters(&vec!["status~=`abc`".to_string()]).is_err(),
            "The filter should not have been created"
        );
        assert!(
            NaiveFilter::from_filters(&vec!["status~=`abc`".to_string()]).is_err(),
            "The filter should not have been created"
        );
    }

    #[test]
    fn valid_filters() {
        assert!(
            NaiveFilter::from_filters(&vec!["method=='get'".to_string()]).is_ok(),
            "The filter should have been created"
        );
        assert!(
            NaiveFilter::from_filters(&vec!["status==`404`".to_string()]).is_ok(),
            "The filter should have been created"
        );
        assert!(
            NaiveFilter::from_filters(&vec!["status==internal.status".to_string()]).is_ok(),
            "The filter should have been created"
        );
        assert!(
            NaiveFilter::from_filters(&vec!["internal.value!=`3.1415962`".to_string()]).is_ok(),
            "The filter should have been created"
        );
    }
}
