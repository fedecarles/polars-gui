use polars::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameMelt {
    pub id_selection: String,
    pub val_selection: String,
    pub id_vars: Vec<String>,
    pub value_vars: Vec<String>,
    pub meltdata: Option<DataFrame>,
    pub display: bool,
}

impl Default for DataFrameMelt {
    fn default() -> Self {
        Self {
            id_selection: String::default(),
            val_selection: String::default(),
            id_vars: Vec::new(),
            value_vars: Vec::new(),
            meltdata: None,
            display: false,
        }
    }
}
