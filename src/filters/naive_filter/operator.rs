use std::sync::Arc;

use serde_json::Value;

use crate::filters::naive_filter::error::NaiveFilterError;

struct GteOperator {}
struct LteOperator {}
struct EqOperator {}
struct NeqOperator {}
struct IeqOperator {}
struct GtOperator {}
struct LtOperator {}

pub(crate) trait Operator: Send + Sync + 'static {
    fn execute(&self, left: &Value, right: &Value) -> Result<bool, NaiveFilterError>;
}

impl Operator for GteOperator {
    fn execute(&self, left: &Value, right: &Value) -> Result<bool, NaiveFilterError> {
        match left {
            Value::Number(n) => {
                return if let Some(integer) = n.as_i64() {
                    Ok(integer >= right.as_i64().unwrap())
                } else {
                    Ok(n.as_f64().unwrap() >= right.as_f64().unwrap())
                }
            },
            _ => Err(
                NaiveFilterError::RuntimeError {
                    reason: format!("The >= operator can only be used for numbers (for example, `2` or `3.1415`, along with quotes). Passed: {:?}, {:?}", left, right)
                }
            )
        }
    }
}

impl Operator for LteOperator {
    fn execute(&self, left: &Value, right: &Value) -> Result<bool, NaiveFilterError> {
        match left {
            Value::Number(n) => {
                return if let Some(integer) = n.as_i64() {
                    Ok(integer <= right.as_i64().unwrap())
                } else {
                    Ok(n.as_f64().unwrap() <= right.as_f64().unwrap())
                }
            },
            _ => Err(
                NaiveFilterError::RuntimeError {
                    reason: format!("The <= operator can only be used for numbers (for example, `2` or `3.1415`, along with quotes). Passed: {:?}, {:?}", left, right)
                }
            )
        }
    }
}
impl Operator for EqOperator {
    fn execute(&self, left: &Value, right: &Value) -> Result<bool, NaiveFilterError> {
        match left {
            Value::Number(n) => {
                return if let Some(integer) = n.as_i64() {
                    Ok(integer == right.as_i64().unwrap())
                } else {
                    Ok(n.as_f64().unwrap() == right.as_f64().unwrap())
                }
            },
            Value::String(s) => {
                return Ok(s.as_str() == right.as_str().unwrap())
            },
            Value::Bool(b) => return Ok(*b == right.as_bool().unwrap()),
            _ => Err(
                NaiveFilterError::RuntimeError {
                    reason: format!("The == operator can only be used for numbers, strings or bools. Passed: {:?}, {:?}", left, right)
                }
            )
        }
    }
}
impl Operator for NeqOperator {
    fn execute(&self, left: &Value, right: &Value) -> Result<bool, NaiveFilterError> {
        match left {
            Value::Number(n) => {
                return if let Some(integer) = n.as_i64() {
                    Ok(integer != right.as_i64().unwrap())
                } else {
                    Ok(n.as_f64().unwrap() != right.as_f64().unwrap())
                }
            },
            Value::String(s) => {
                return Ok(s.as_str() != right.as_str().unwrap())
            },
            Value::Bool(b) => return Ok(*b != right.as_bool().unwrap()),
            _ => Err(
                NaiveFilterError::RuntimeError {
                    reason: format!("The != operator can only be used for numbers, strings or bools. Passed: {:?}, {:?}", left, right)
                }
            )
        }
    }
}
impl Operator for IeqOperator {
    fn execute(&self, left: &Value, right: &Value) -> Result<bool, NaiveFilterError> {
        Ok(left
            .as_str()
            .unwrap()
            .eq_ignore_ascii_case(right.as_str().unwrap()))
    }
}
impl Operator for GtOperator {
    fn execute(&self, left: &Value, right: &Value) -> Result<bool, NaiveFilterError> {
        match left {
            Value::Number(n) => {
                return if let Some(integer) = n.as_i64() {
                    Ok(integer > right.as_i64().unwrap())
                } else {
                    Ok(n.as_f64().unwrap() > right.as_f64().unwrap())
                }
            },
            _ => Err(
                NaiveFilterError::RuntimeError {
                    reason: format!("The > operator can only be used for numbers (for example, `2` or `3.1415`, along with quotes). Passed: {:?}, {:?}", left, right)
                }
            )
        }
    }
}
impl Operator for LtOperator {
    fn execute(&self, left: &Value, right: &Value) -> Result<bool, NaiveFilterError> {
        match left {
            Value::Number(n) => {
                return if let Some(integer) = n.as_i64() {
                    Ok(integer < right.as_i64().unwrap())
                } else {
                    Ok(n.as_f64().unwrap() < right.as_f64().unwrap())
                }
            },
            _ => Err(
                NaiveFilterError::RuntimeError {
                    reason: format!("The < operator can only be used for numbers (for example, `2` or `3.1415`, along with quotes). Passed: {:?}, {:?}", left, right)
                }
            )
        }
    }
}

pub(crate) type OperatorRef = Arc<dyn Operator>;

pub(crate) fn get_operator(operator_str: &str) -> Result<OperatorRef, NaiveFilterError> {
    match operator_str {
        ">=" => Ok(Arc::new(GteOperator {})),
        "<=" => Ok(Arc::new(LteOperator {})),
        "==" => Ok(Arc::new(EqOperator {})),
        "!=" => Ok(Arc::new(NeqOperator {})),
        "~=" => Ok(Arc::new(IeqOperator {})),
        ">" => Ok(Arc::new(GtOperator {})),
        "<" => Ok(Arc::new(LtOperator {})),
        _ => Err(NaiveFilterError::RuntimeError {
            reason: format!("There is no operand {}", operator_str),
        }),
    }
}
