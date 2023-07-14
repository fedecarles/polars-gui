use polars::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameSummary {
    pub summary_data: Option<DataFrame>,
    pub display: bool,
}

impl Default for DataFrameSummary {
    fn default() -> Self {
        Self {
            summary_data: None,
            display: false,
        }
    }
}
