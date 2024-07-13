use serde_json::Value;

use crate::filters::FilterError;

/// Trait for implementing a filter mechanism
pub trait Filter: Send {
    /// Constructor
    fn from_filters(filters: &Vec<String>) -> Result<Self, FilterError> where Self: Sized;

    /// A function that filters a message. If any of the filters fail, it throws an error;
    /// if all filters pass, it returns nothing.
    fn filter(&self, message: &Value) -> Result<(), FilterError>;
}