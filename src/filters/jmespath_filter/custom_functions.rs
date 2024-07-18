use std::convert::TryFrom;
use std::sync::Arc;

use jmespatch::functions::{ArgumentType, CustomFunction, Signature};
use jmespatch::{Context, ErrorReason, JmespathError, Rcvar, Variable};

/// Custom function to compare two string values in a case-insensitive manner
fn eq_ignore_case(args: &[Rcvar], context: &mut Context) -> Result<Rcvar, JmespathError> {
    let s = match args[0].as_string() {
        None => {
            return Err(JmespathError::new(
                context.expression,
                context.offset,
                ErrorReason::Parse("first variable must be string".to_string()),
            ))
        }
        Some(s) => s,
    };

    let p = match args[1].as_string() {
        None => {
            return Err(JmespathError::new(
                context.expression,
                context.offset,
                ErrorReason::Parse("second variable must be string".to_string()),
            ))
        }
        Some(p) => p,
    };

    let var = Variable::try_from(serde_json::Value::Bool(s.eq_ignore_ascii_case(p)))?;

    Ok(Arc::new(var))
}

pub fn create_eq_ignore_case_function() -> CustomFunction {
    CustomFunction::new(
        Signature::new(vec![ArgumentType::String, ArgumentType::String], None),
        Box::new(eq_ignore_case),
    )
}
