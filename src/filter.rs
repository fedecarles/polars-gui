use polars::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub enum FilterOps {
    EqualNum,
    EqualStr,
    GreaterThan,
    GreaterEqualThan,
    LowerThan,
    LowerEqualThan,
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameFilter {
    pub column: String,
    pub operation: FilterOps,
    pub value: String,
    pub inplace: bool,
    pub filtered_data: Option<DataFrame>,
}

impl Default for DataFrameFilter {
    fn default() -> Self {
        Self {
            column: String::from(""),
            operation: FilterOps::EqualNum,
            value: String::from(""),
            inplace: false,
            filtered_data: None,
        }
    }
}
