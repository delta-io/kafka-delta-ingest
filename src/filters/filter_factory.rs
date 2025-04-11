use crate::filters::{Filter, FilterError, JmespathFilter, NaiveFilter};

/// Filter options
#[derive(Clone, Debug)]
pub enum FilterEngine {
    /// Filter for simple comparisons that works a little faster
    Naive,
    /// Filter for complex comparisons
    Jmespath,
}

/// Factory for creating and managing filters
pub struct FilterFactory {}
impl FilterFactory {
    /// Factory for creating filter instances
    pub fn try_build(
        filter_engine: &FilterEngine,
        filters: &[String],
    ) -> Result<Box<dyn Filter>, FilterError> {
        match filter_engine {
            FilterEngine::Naive => match NaiveFilter::from_filters(filters) {
                Ok(f) => Ok(Box::new(f)),
                Err(e) => Err(e),
            },
            FilterEngine::Jmespath => match JmespathFilter::from_filters(filters) {
                Ok(f) => Ok(Box::new(f)),
                Err(e) => Err(e),
            },
        }
    }
}
