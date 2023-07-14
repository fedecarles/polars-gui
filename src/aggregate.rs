use polars::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Mean,
    Median,
    Min,
    Max,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameAggregate {
    pub grp_selection: String,
    pub agg_selection: String,
    pub groupby: Vec<String>,
    pub aggcols: Vec<String>,
    pub aggfunc: AggFunc,
    pub aggdata: Option<DataFrame>,
    pub display: bool,
}

impl Default for DataFrameAggregate {
    fn default() -> Self {
        Self {
            grp_selection: String::default(),
            agg_selection: String::default(),
            groupby: Vec::new(),
            aggcols: Vec::new(),
            aggfunc: AggFunc::Count,
            aggdata: None,
            display: false,
        }
    }
}
