use serde_json::{json, Value};

use crate::filters::naive_filter::error::NaiveFilterError;

/// Container to store the path to the value or the value itself for later comparison
pub(super) struct NaiveFilterOperand {
    pub value: Option<Value>,
    pub path: Option<Vec<String>>,
}

impl NaiveFilterOperand {
    fn new(value: Option<Value>, path: Option<String>) -> Result<Self, NaiveFilterError> {
        if value.is_none() && path.is_none() {
            return Err(NaiveFilterError::PrepareError {
                reason: "Cannot create expression without path or value".to_string(),
            });
        };

        if value.is_some() {
            return Ok(Self { value, path: None });
        }

        let path: Vec<String> = path.unwrap().split('.').map(str::to_string).collect();
        Ok(Self {
            value,
            path: Some(path),
        })
    }

    pub(crate) fn from_str(operand_str: &str) -> Result<Self, NaiveFilterError> {
        let operand_str = operand_str.trim();

        match operand_str.chars().next() {
            // number
            Some('`') => {
                if !operand_str.ends_with('`') {
                    return Err(NaiveFilterError::PrepareError {
                        reason: "To filter by number, the number must begin and end with `"
                            .to_string(),
                    });
                }
                NaiveFilterOperand::new(serde_json::from_str(operand_str.trim_matches('`'))?, None)
            }
            // string
            Some('\'') => {
                if !operand_str.ends_with('\'') {
                    return Err(NaiveFilterError::PrepareError {
                        reason: "To filter by string, the string must begin and end with '"
                            .to_string(),
                    });
                }
                NaiveFilterOperand::new(Some(json!(operand_str.trim_matches('\''))), None)
            }
            // path to attribute via dot
            _ => NaiveFilterOperand::new(None, Some(operand_str.to_string())),
        }
    }
    fn is_path(&self) -> bool {
        self.path.is_some()
    }

    pub(crate) fn get_value<'a>(&'a self, message: &'a Value) -> &Value {
        return if self.is_path() {
            let mut path_iter = self.path.as_ref().unwrap().iter();
            let mut cursor: &Value = &message[path_iter.next().unwrap()];
            for p in path_iter {
                cursor = &cursor[p];
            }
            return cursor;
        } else {
            self.value.as_ref().unwrap()
        };
    }
}
