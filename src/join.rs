use polars::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameJoin {
    pub df_selection: String,
    pub df_list: Vec<String>,
    pub left_on_selection: String,
    pub right_on_selection: String,
    pub right_on_cols: Vec<String>,
    pub how: JoinType,
    pub joindata: Option<DataFrame>,
    pub join: bool,
    pub inplace: bool,
}

impl Default for DataFrameJoin {
    fn default() -> Self {
        Self {
            df_selection: String::default(),
            df_list: Vec::new(),
            left_on_selection: String::default(),
            right_on_selection: String::default(),
            right_on_cols: Vec::new(),
            how: JoinType::Inner,
            joindata: None,
            join: false,
            inplace: false,
        }
    }
}
