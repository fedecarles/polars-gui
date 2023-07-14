use crate::aggregate::*;
use crate::filter::*;
use crate::join::DataFrameJoin;
use crate::melt::DataFrameMelt;
use crate::summary::DataFrameSummary;
use crate::utils::{display_dataframe, get_container};
use egui::{ComboBox, Grid, TextEdit, Window};
use polars::prelude::*;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameContainer {
    pub title: String,
    pub shape: (usize, usize),
    pub data: DataFrame,
    pub summary: DataFrameSummary,
    pub columns: Vec<String>,
    pub data_display: bool,
    pub is_open: bool,
    pub show_datatypes: bool,
    pub filter: DataFrameFilter,
    pub aggregate: DataFrameAggregate,
    pub melt: DataFrameMelt,
    pub join: DataFrameJoin,
}

impl DataFrameContainer {
    pub fn new(df: DataFrame, title: &str) -> Self {
        Self {
            title: String::from(format!("{}", String::from(title))),
            shape: df.shape(),
            data: df.clone(),
            summary: DataFrameSummary::default(),
            columns: df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect(),
            data_display: false,
            is_open: true,
            show_datatypes: false,
            filter: DataFrameFilter::default(),
            aggregate: DataFrameAggregate::default(),
            melt: DataFrameMelt::default(),
            join: DataFrameJoin::default(),
        }
    }

    pub fn filter_dataframe(
        &mut self,
        df: DataFrame,
        column: &str,
        operation: &FilterOps,
        value: &str,
    ) -> Result<DataFrame, PolarsError> {
        let parsed_number = value.parse::<f64>().unwrap_or_default();
        let parsed_string = value.parse::<String>().unwrap_or_default();
        match operation {
            FilterOps::EqualNum => df
                .lazy()
                .filter(col(column).eq(lit(parsed_number)))
                .collect(),
            FilterOps::EqualStr => df
                .lazy()
                .filter(col(column).eq(lit(parsed_string)))
                .collect(),
            FilterOps::GreaterThan => df
                .lazy()
                .filter(col(column).gt(lit(parsed_number)))
                .collect(),
            FilterOps::GreaterEqualThan => df
                .lazy()
                .filter(col(column).gt_eq(lit(parsed_number)))
                .collect(),
            FilterOps::LowerThan => df
                .lazy()
                .filter(col(column).lt(lit(parsed_number)))
                .collect(),
            FilterOps::LowerEqualThan => df
                .lazy()
                .filter(col(column).lt_eq(lit(parsed_number)))
                .collect(),
            FilterOps::IsNull => df.lazy().filter(col(column).is_null()).collect(),
            FilterOps::IsNotNull => df.lazy().filter(col(column).is_not_null()).collect(),
        }
    }

    pub fn aggregate_dataframe(
        &mut self,
        df: DataFrame,
        groupby: &Vec<&str>,
        aggcols: &Vec<&str>,
        aggfunc: &AggFunc,
    ) -> Result<DataFrame, PolarsError> {
        match aggfunc {
            AggFunc::Count => df
                .lazy()
                .groupby(groupby)
                .agg([cols(aggcols).count()])
                .collect(),
            AggFunc::Sum => df
                .lazy()
                .groupby(groupby)
                .agg([cols(aggcols).sum()])
                .collect(),
            AggFunc::Mean => df
                .lazy()
                .groupby(groupby)
                .agg([cols(aggcols).mean()])
                .collect(),
            AggFunc::Median => df
                .lazy()
                .groupby(groupby)
                .agg([cols(aggcols).median()])
                .collect(),
            AggFunc::Min => df
                .lazy()
                .groupby(groupby)
                .agg([cols(aggcols).min()])
                .collect(),
            AggFunc::Max => df
                .lazy()
                .groupby(groupby)
                .agg([cols(aggcols).max()])
                .collect(),
        }
    }
    pub fn join_dataframe(
        &mut self,
        container: &mut DataFrameContainer,
        frame_vec: &mut Vec<HashMap<String, DataFrameContainer>>,
        join_vec: &Vec<HashMap<String, DataFrameContainer>>,
    ) {
        if !container.join.df_selection.is_empty() {
            let join_df = get_container(&join_vec, &container.join.df_selection);
            if let Some(j_df) = join_df {
                let df = &container.data;
                let joined_df = df.join(
                    &j_df.data,
                    [&container.join.left_on_selection],
                    [&container.join.right_on_selection],
                    container.join.how.clone(),
                    None,
                );
                if let Ok(joined) = joined_df {
                    let joined_title = format!("joined_{}{}", container.title, &frame_vec.len());
                    let joined_container = DataFrameContainer::new(joined.clone(), &joined_title);
                    match container.join.inplace {
                        false => {
                            let mut join_hash = HashMap::new();
                            join_hash.insert(joined_title, joined_container);
                            frame_vec.push(join_hash);
                            // cleanup. set original filtered data back to None
                            container.filter.filtered_data = None;
                        }
                        true => {
                            container.data = joined.clone();
                            container.shape = joined.shape();
                            container.summary.summary_data = joined.describe(None).ok();
                        }
                    }
                }
                container.join.join = false;
            } else {
                println!("DataFrameContainer could not be found");
            }
        }
    }
    pub fn show(&mut self, ctx: &egui::Context) {
        let window = Window::new(format!("🗖 {}", &self.title));
        let mut is_open = std::mem::take(&mut self.is_open); // temporary move is_open out of self
                                                             // to allow the show_content call.

        window
            .open(&mut is_open)
            .scroll2([true, true])
            .auto_sized()
            .resizable(false)
            .show(ctx, |ui| self.show_content(ctx, ui));

        self.is_open = is_open; // put is_open back on self.
    }

    fn show_content(&mut self, ctx: &egui::Context, ui: &mut egui::Ui) {
        Grid::new("main_grid")
            .num_columns(2)
            .spacing([40.0, 4.0])
            .striped(true)
            .show(ui, |ui| {
                ui.label("Shape: ");
                ui.label(String::from(format!("{:?}", &self.shape)));
                ui.end_row();
                ui.label("Data: ");
                let btn = ui.button("View");
                if btn.clicked() {
                    self.data_display = !&self.data_display;
                }
                if self.data_display {
                    Window::new(format!("{}{}", String::from("Data: "), &self.title))
                        .open(&mut self.data_display)
                        .show(ctx, |ui| display_dataframe(&self.data, ui));
                }
                ui.end_row();
                ui.label("Summary: ");
                let btn = ui.button("View");
                if btn.clicked() {
                    self.summary.display = !&self.summary.display;
                    if self.summary.summary_data.is_none() {
                        self.summary.summary_data = self.data.describe(None).ok();
                    }
                }
                if self.summary.display {
                    let binding = self.summary.summary_data.clone().unwrap_or_default();
                    Window::new(format!("{}{}", String::from("Summary: "), &self.title))
                        .open(&mut self.summary.display)
                        .scroll2([true, true])
                        .show(ctx, |ui| display_dataframe(&binding, ui));
                }
                ui.end_row();
                ui.label("Data Types:");
                if ui.button("View").clicked() {
                    self.show_datatypes = !self.show_datatypes;
                }
                if self.show_datatypes {
                    let dtypes: Vec<String> = self
                        .data
                        .dtypes()
                        .to_vec()
                        .iter()
                        .map(|d| d.to_string())
                        .collect();
                    let dtypes_df = df!(
                        "Columns" => &self.columns,
                        "Dtype" => dtypes.to_vec()
                    )
                    .unwrap_or_default();
                    Window::new(format!("{}{}", String::from("Data Types: "), &self.title))
                        .open(&mut self.show_datatypes)
                        .show(ctx, |ui| display_dataframe(&dtypes_df, ui));
                }
                ui.end_row();
            });
        ui.add_space(15.0);
        ui.label(egui::RichText::new("Data Transformations").text_style(egui::TextStyle::Heading));
        ui.collapsing("Filter", |ui| {
            ui.horizontal(|ui| {
                ui.radio_value(&mut self.filter.inplace, false, "New");
                ui.radio_value(&mut self.filter.inplace, true, "In Place");
            });
            ui.horizontal(|ui| {
                ComboBox::from_label("is")
                    .selected_text(&self.filter.column)
                    .show_ui(ui, |ui| {
                        for col in &self.columns {
                            ui.selectable_value(&mut self.filter.column, col.to_owned(), col);
                        }
                    });
                ComboBox::from_label("than/to")
                    .selected_text(format!("{:?}", &self.filter.operation))
                    .show_ui(ui, |ui| {
                        ui.selectable_value(
                            &mut self.filter.operation,
                            FilterOps::EqualNum,
                            "EqualNum",
                        );
                        ui.selectable_value(
                            &mut self.filter.operation,
                            FilterOps::EqualStr,
                            "EqualStr",
                        );
                        ui.selectable_value(
                            &mut self.filter.operation,
                            FilterOps::GreaterThan,
                            "GreaterThan",
                        );
                        ui.selectable_value(
                            &mut self.filter.operation,
                            FilterOps::GreaterEqualThan,
                            "GreaterEqualThan",
                        );
                        ui.selectable_value(
                            &mut self.filter.operation,
                            FilterOps::LowerThan,
                            "LowerThan",
                        );
                        ui.selectable_value(
                            &mut self.filter.operation,
                            FilterOps::LowerEqualThan,
                            "LowerEqualThan",
                        );
                        ui.selectable_value(&mut self.filter.operation, FilterOps::IsNull, "Null");
                        ui.selectable_value(
                            &mut self.filter.operation,
                            FilterOps::IsNotNull,
                            "IsNotNull",
                        );
                    });
                ui.add(TextEdit::singleline(&mut self.filter.value).desired_width(100.0));
                if ui.button("Filter").clicked() {
                    let f_df = self.filter_dataframe(
                        self.data.clone(),
                        &self.filter.column.clone(),
                        &self.filter.operation.clone(),
                        &self.filter.value.clone(),
                    );
                    if f_df.is_ok() {
                        self.filter.filtered_data = f_df.ok();
                    } else {
                        self.data = self.data.clone()
                    };
                }
            })
        });
        ui.collapsing("Aggregate", |ui| {
            ui.label("Group by:");
            ui.horizontal(|ui| {
                ComboBox::new("Grp", "")
                    .selected_text(&self.aggregate.grp_selection)
                    .show_ui(ui, |ui| {
                        for col in &self.columns {
                            ui.selectable_value(
                                &mut self.aggregate.grp_selection,
                                col.to_owned(),
                                col,
                            );
                        }
                    });
                if ui.button("Add").clicked() {
                    if !self
                        .aggregate
                        .groupby
                        .contains(&self.aggregate.grp_selection)
                    {
                        self.aggregate
                            .groupby
                            .push(self.aggregate.grp_selection.clone());
                    }
                }
            });
            ui.label(format!("Selected: {:?}", &self.aggregate.groupby));
            ui.label("Columns: ");
            ui.horizontal(|ui| {
                ComboBox::new("Agg", "")
                    .selected_text(&self.aggregate.agg_selection)
                    .show_ui(ui, |ui| {
                        for col in &self.columns {
                            ui.selectable_value(
                                &mut self.aggregate.agg_selection,
                                col.to_owned(),
                                col,
                            );
                        }
                    });
                if ui.button("Add").clicked() {
                    if !self
                        .aggregate
                        .aggcols
                        .contains(&self.aggregate.agg_selection)
                    {
                        self.aggregate
                            .aggcols
                            .push(self.aggregate.agg_selection.clone());
                    }
                }
            });
            ui.label(format!("Selected: {:?}", &self.aggregate.aggcols));
            ui.label("Metric: ");
            ui.horizontal(|ui| {
                ui.radio_value(&mut self.aggregate.aggfunc, AggFunc::Count, "Count");
                ui.radio_value(&mut self.aggregate.aggfunc, AggFunc::Sum, "Sum");
                ui.radio_value(&mut self.aggregate.aggfunc, AggFunc::Mean, "Mean");
            });
            ui.horizontal(|ui| {
                ui.radio_value(&mut self.aggregate.aggfunc, AggFunc::Median, "Median");
                ui.radio_value(&mut self.aggregate.aggfunc, AggFunc::Min, "Min");
                ui.radio_value(&mut self.aggregate.aggfunc, AggFunc::Max, "Max");
            });

            if ui.button("Aggregate").clicked() {
                self.aggregate.display = true;
                let binding = self.aggregate.groupby.clone();
                let binding2 = self.aggregate.aggcols.clone();
                let binding3 = self.aggregate.aggfunc.clone();
                let str_gp: &Vec<&str> = &binding.iter().map(|s| s.as_str()).collect();
                let str_agg: &Vec<&str> = &binding2.iter().map(|s| s.as_str()).collect();

                let aggdf = self.aggregate_dataframe(self.data.clone(), str_gp, str_agg, &binding3);
                if let Ok(aggregated) = aggdf {
                    self.aggregate.aggdata = Some(aggregated);
                }
            }
            if self.aggregate.display {
                let binding = self.aggregate.aggdata.clone().unwrap();
                Window::new(format!("{}{}", String::from("Aggregation: "), &self.title))
                    .open(&mut self.aggregate.display)
                    .show(ctx, |ui| {
                        display_dataframe(&binding.clone(), ui);
                    });
            }
        });
        ui.collapsing("Join", |ui| {
            ui.horizontal(|ui| {
                ui.radio_value(&mut self.join.inplace, false, "New");
                ui.radio_value(&mut self.join.inplace, true, "In Place");
            });
            ComboBox::new("dfs", "")
                .selected_text(&self.join.df_selection)
                .show_ui(ui, |ui| {
                    for col in &self.join.df_list {
                        ui.selectable_value(&mut self.join.df_selection, col.to_owned(), col);
                    }
                });
            ComboBox::new("left_on", "")
                .selected_text(&self.join.left_on_selection)
                .show_ui(ui, |ui| {
                    for col in &self.columns {
                        ui.selectable_value(&mut self.join.left_on_selection, col.to_owned(), col);
                    }
                });
            ComboBox::new("right_on", "")
                .selected_text(&self.join.right_on_selection)
                .show_ui(ui, |ui| {
                    for col in &self.join.right_on_cols {
                        ui.selectable_value(&mut self.join.right_on_selection, col.to_owned(), col);
                    }
                });
            ui.horizontal(|ui| {
                ui.radio_value(&mut self.join.how, JoinType::Inner, "Inner");
                ui.radio_value(&mut self.join.how, JoinType::Left, "Left");
                ui.radio_value(&mut self.join.how, JoinType::Outer, "Outer");
                ui.radio_value(&mut self.join.how, JoinType::Cross, "Cross");
            });
            if ui.button("Join").clicked() {
                self.join.join = !self.join.join
            }
        });
        ui.collapsing("Melt", |ui| {
            ui.label("ID Vars: ");
            ui.horizontal(|ui| {
                ComboBox::new("Idvars", "")
                    .selected_text(&self.melt.id_selection)
                    .show_ui(ui, |ui| {
                        for col in &self.columns {
                            ui.selectable_value(&mut self.melt.id_selection, col.to_owned(), col);
                        }
                    });
                if ui.button("Add").clicked() {
                    if !self.melt.id_vars.contains(&self.melt.id_selection) {
                        self.melt.id_vars.push(self.melt.id_selection.clone());
                    }
                }
            });
            ui.label(format!("Selected: {:?}", &self.melt.id_vars));
            ui.label("Value Vars: ");
            ui.horizontal(|ui| {
                ComboBox::new("Valvars", "")
                    .selected_text(&self.melt.val_selection)
                    .show_ui(ui, |ui| {
                        for col in &self.columns {
                            ui.selectable_value(&mut self.melt.val_selection, col.to_owned(), col);
                        }
                    });
                if ui.button("Add").clicked() {
                    if !self.melt.value_vars.contains(&self.melt.val_selection) {
                        self.melt.value_vars.push(self.melt.val_selection.clone());
                    }
                }
            });
            ui.label(format!("Selected: {:?}", &self.melt.value_vars));
            if ui.button("Melt").clicked() {
                self.melt.display = true;
                let melted_df = self.data.melt(&self.melt.id_vars, &self.melt.value_vars);
                if melted_df.is_ok() {
                    self.melt.meltdata = melted_df.ok();
                }
            }
            if self.melt.display {
                let binding = self.melt.meltdata.clone().unwrap_or_default();
                Window::new(format!("{}{}", String::from("Melt: "), &self.title))
                    .open(&mut self.melt.display)
                    .show(ctx, |ui| {
                        display_dataframe(&binding, ui);
                    });
            }
        });
    }
}
