use egui::{ComboBox, Grid, TextEdit, Window};
use egui_extras::{Column, TableBuilder};
use polars::prelude::*;
use rfd::FileDialog;
use std::collections::HashMap;
use std::fmt::Debug;

/// We derive Deserialize/Serialize so we can persist app state on shutdown.
#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(default)] // if we add new fields, give them default values when deserializing old state
pub struct App {
    label: String,
    // this how you opt-out of serialization of a member
    #[serde(skip)]
    version: f32,
    #[serde(skip)]
    frames: Vec<HashMap<String, DataFrameContainer>>,
    titles: Vec<String>,
    df_cols: HashMap<String, Vec<String>>,
}

impl Default for App {
    fn default() -> Self {
        Self {
            label: "Polars GUI".to_owned(),
            version: 0.1,
            frames: Vec::new(),
            titles: Vec::new(),
            df_cols: HashMap::default(),
        }
    }
}

impl App {
    /// Called once before the first frame.
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        // This is also where you can customize the look and feel of egui using
        // `cc.egui_ctx.set_visuals` and `cc.egui_ctx.set_fonts`.

        // Load previous app state (if any).
        // Note that you must enable the `persistence` feature for this to work.
        //if let Some(storage) = cc.storage {
        //    return eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default();
        //}
        Default::default()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameContainer {
    title: String,
    shape: (usize, usize),
    data: DataFrame,
    //summary
    summary: DataFrameSummary,
    columns: Vec<String>,
    data_display: bool,
    is_open: bool,
    show_datatypes: bool,
    // filter
    filter: DataFrameFilter,
    // aggregate
    aggregate: DataFrameAggregate,
    // melt
    melt: DataFrameMelt,
    // join
    join: DataFrameJoin,
}

impl DataFrameContainer {
    fn new(df: DataFrame, title: &str) -> Self {
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

    fn show(&mut self, ctx: &egui::Context) {
        let window = Window::new(format!("🗖 {}", &self.title));

        window
            .open(&mut self.is_open)
            .scroll2([true, true])
            //.resize(|r| r.max_size((1920.0, 1080.0)))
            .auto_sized()
            .resizable(false)
            .show(ctx, |ui| {
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
                ui.label(
                    egui::RichText::new("Data Transformations")
                        .text_style(egui::TextStyle::Heading),
                );
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
                                    ui.selectable_value(
                                        &mut self.filter.column,
                                        col.to_owned(),
                                        col,
                                    );
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
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOps::IsNull,
                                    "Null",
                                );
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOps::IsNotNull,
                                    "IsNotNull",
                                );
                            });
                        ui.add(TextEdit::singleline(&mut self.filter.value).desired_width(100.0));
                        if ui.button("Filter").clicked() {
                            let f_df = filter_dataframe(
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
                        let str_gp: Vec<&str> =
                            self.aggregate.groupby.iter().map(|s| s.as_str()).collect();
                        let str_agg: Vec<&str> =
                            self.aggregate.aggcols.iter().map(|s| s.as_str()).collect();

                        let aggdf = aggregate_dataframe(
                            self.data.clone(),
                            str_gp,
                            str_agg,
                            &self.aggregate.aggfunc,
                        );
                        if let Ok(aggregated) = aggdf {
                            self.aggregate.aggdata = Some(aggregated);
                        }
                    }
                    if self.aggregate.display {
                        let binding = self.aggregate.aggdata.clone().unwrap_or_default();
                        Window::new(format!("{}{}", String::from("Aggregation: "), &self.title))
                            .open(&mut self.aggregate.display)
                            .show(ctx, |ui| {
                                display_dataframe(&binding, ui);
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
                                ui.selectable_value(
                                    &mut self.join.df_selection,
                                    col.to_owned(),
                                    col,
                                );
                            }
                        });
                    ComboBox::new("left_on", "")
                        .selected_text(&self.join.left_on_selection)
                        .show_ui(ui, |ui| {
                            for col in &self.columns {
                                ui.selectable_value(
                                    &mut self.join.left_on_selection,
                                    col.to_owned(),
                                    col,
                                );
                            }
                        });
                    ComboBox::new("right_on", "")
                        .selected_text(&self.join.right_on_selection)
                        .show_ui(ui, |ui| {
                            for col in &self.join.right_on_cols {
                                ui.selectable_value(
                                    &mut self.join.right_on_selection,
                                    col.to_owned(),
                                    col,
                                );
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
                                    ui.selectable_value(
                                        &mut self.melt.id_selection,
                                        col.to_owned(),
                                        col,
                                    );
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
                                    ui.selectable_value(
                                        &mut self.melt.val_selection,
                                        col.to_owned(),
                                        col,
                                    );
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
            });
    }
}

impl eframe::App for App {
    /// Called by the frame work to save state before shutdown.
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        eframe::set_value(storage, eframe::APP_KEY, self);
    }

    /// Called each time the UI needs repainting, which may be many times per second.
    /// Put your widgets into a `SidePanel`, `TopPanel`, `CentralPanel`, `Window` or `Area`.
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        #[cfg(not(target_arch = "wasm32"))] // no File->Quit on web pages!
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            // The top panel is often a good place for a menu bar:
            egui::menu::bar(ui, |ui| {
                ui.menu_button("New", |ui| {
                    if ui.button("DataFrame").clicked() {
                        if let Some(path) = FileDialog::new().pick_file() {
                            let df: DataFrame = CsvReader::from_path(&path)
                                .unwrap()
                                .infer_schema(Some(10000))
                                .finish()
                                .unwrap();
                            let file_name: &str = &path.file_name().unwrap().to_str().unwrap();
                            let mut hash = HashMap::new();
                            hash.insert(
                                file_name.to_string(),
                                DataFrameContainer::new(df.clone(), file_name),
                            );
                            self.frames.push(hash);
                            let cols = df
                                .clone()
                                .get_column_names()
                                .iter()
                                .map(|c| c.to_string())
                                .collect();
                            self.df_cols.insert(String::from(file_name), cols);
                            self.titles.push(file_name.to_string());
                        }
                    }
                });
                ui.menu_button("App", |ui| {
                    if ui.button("Quit").clicked() {
                        _frame.close();
                    }
                });
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            let mut temp_frames = Vec::new(); // Temporary vector to hold the filtered frames
            let temp_joins = self.frames.clone();
            let nr_frames = &self.frames.len();

            for map in self.frames.iter_mut() {
                for (_key, val) in map {
                    let frame_refcell = val;
                    frame_refcell.show(ctx);

                    // Filter creates a new DataFrameContainer. InPlace option updates the
                    // existing container with the new one. The New option displays the filtered
                    // data in a new window.
                    // TODO: revise/re-factor filter functionality
                    if frame_refcell.filter.filtered_data.is_some() {
                        let filtered_title =
                            format!("filtered_{}{}", &frame_refcell.title, &nr_frames);
                        let filtered_df = DataFrameContainer::new(
                            frame_refcell
                                .clone()
                                .filter
                                .filtered_data
                                .unwrap_or_default(),
                            &filtered_title,
                        );
                        match frame_refcell.filter.inplace {
                            false => {
                                let mut filter_hash = HashMap::new();
                                filter_hash.insert(
                                    format!("filtered_{}", &frame_refcell.title),
                                    filtered_df,
                                );
                                temp_frames.push(filter_hash);
                                // cleanup. set original filtered data back to None
                                frame_refcell.filter.filtered_data = None;
                            }
                            true => {
                                frame_refcell.data = filtered_df.data.clone();
                                frame_refcell.shape = filtered_df.data.shape().clone();
                                frame_refcell.summary.summary_data =
                                    filtered_df.data.clone().describe(None).ok();
                            }
                        }
                    }

                    // Join requires the selection of another DataFrameContainer in the frames list
                    // and the mapped columns stored in df_cols.
                    frame_refcell.join.df_list = self.titles.clone();
                    let df_cols = self.df_cols.get(&frame_refcell.join.df_selection);

                    if df_cols.is_some() {
                        frame_refcell.join.right_on_cols =
                            df_cols.unwrap_or(&Vec::new()).to_owned();
                    }

                    if frame_refcell.join.join {
                        join_dataframe(frame_refcell, &mut temp_frames, temp_joins.clone());
                    }
                }
            }
            // Push the filtered frames into self.frames after the nested loops
            self.frames.extend(temp_frames);

            egui::warn_if_debug_build(ui);
        });
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameSummary {
    summary_data: Option<DataFrame>,
    display: bool,
}

impl Default for DataFrameSummary {
    fn default() -> Self {
        Self {
            summary_data: None,
            display: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameFilter {
    column: String,
    operation: FilterOps,
    value: String,
    inplace: bool,
    filtered_data: Option<DataFrame>,
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

#[derive(Clone, Debug, PartialEq)]
enum FilterOps {
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
enum AggFunc {
    Count,
    Sum,
    Mean,
    Median,
    Min,
    Max,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameAggregate {
    grp_selection: String,
    agg_selection: String,
    groupby: Vec<String>,
    aggcols: Vec<String>,
    aggfunc: AggFunc,
    aggdata: Option<DataFrame>,
    display: bool,
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

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameMelt {
    id_selection: String,
    val_selection: String,
    id_vars: Vec<String>,
    value_vars: Vec<String>,
    meltdata: Option<DataFrame>,
    display: bool,
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

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameJoin {
    df_selection: String,
    df_list: Vec<String>,
    left_on_selection: String,
    right_on_selection: String,
    right_on_cols: Vec<String>,
    how: JoinType,
    joindata: Option<DataFrame>,
    join: bool,
    inplace: bool,
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

fn aggregate_dataframe(
    df: DataFrame,
    groupby: Vec<&str>,
    aggcols: Vec<&str>,
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

fn filter_dataframe(
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

fn display_dataframe(df: &DataFrame, ui: &mut egui::Ui) {
    let nr_cols = df.width();
    let nr_rows = df.height();
    let cols = &df.get_column_names();

    TableBuilder::new(ui)
        .column(Column::auto())
        .columns(Column::auto().clip(true), nr_cols)
        .striped(true)
        .resizable(true)
        .header(20.0, |mut header| {
            header.col(|ui| {
                ui.label(format!("{}", "Row"));
            });
            for head in cols {
                header.col(|ui| {
                    ui.heading(format!("{}", head));
                });
            }
        })
        .body(|body| {
            body.rows(10.0, nr_rows, |row_index, mut row| {
                row.col(|ui| {
                    ui.label(format!("{}", row_index));
                });
                for col in cols {
                    row.col(|ui| {
                        if let Ok(column) = &df.column(col) {
                            if let Ok(value) = column.get(row_index) {
                                ui.label(format!("{}", value).replace('"', ""));
                            }
                        }
                    });
                }
            });
        });
}

fn get_container(
    containers: &Vec<HashMap<String, DataFrameContainer>>,
    title: &str,
) -> Option<DataFrameContainer> {
    for map in containers {
        if let Some(container) = map.get(title) {
            return Some(container.clone());
        }
    }
    None
}

fn join_dataframe(
    container: &mut DataFrameContainer,
    frame_vec: &mut Vec<HashMap<String, DataFrameContainer>>,
    join_vec: Vec<HashMap<String, DataFrameContainer>>,
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
