use egui::{ComboBox, Grid, TextEdit, Window};
use egui_extras::{Column, TableBuilder};
use polars::prelude::*;
use rfd::FileDialog;
use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;

/// We derive Deserialize/Serialize so we can persist app state on shutdown.
#[derive(serde::Deserialize, serde::Serialize, Debug)]
#[serde(default)] // if we add new fields, give them default values when deserializing old state
pub struct TemplateApp {
    label: String,

    // this how you opt-out of serialization of a member
    #[serde(skip)]
    value: f32,
    #[serde(skip)]
    frames: Option<Vec<Rc<RefCell<DataFrameContainer>>>>,
}

impl Default for TemplateApp {
    fn default() -> Self {
        Self {
            label: "Polars GUI".to_owned(),
            value: 0.1,
            frames: Some(Vec::new()),
        }
    }
}

impl TemplateApp {
    /// Called once before the first frame.
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        // This is also where you can customize the look and feel of egui using
        // `cc.egui_ctx.set_visuals` and `cc.egui_ctx.set_fonts`.

        // Load previous app state (if any).
        // Note that you must enable the `persistence` feature for this to work.
        if let Some(storage) = cc.storage {
            return eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default();
        }
        Default::default()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameContainer {
    title: String,
    shape: (usize, usize),
    data: DataFrame,
    //summary
    summary_data: DataFrame,
    summary: DataFrameSummary,
    columns: Vec<String>,
    data_display: bool,
    is_open: bool,
    show_datatypes: bool,
    // filter
    filter: DataFrameFilter,
    // aggregate
    aggregate: DataFrameAggregate,
    melt: DataFrameMelt,
}

impl DataFrameContainer {
    fn new(df: DataFrame, title: &str) -> Self {
        Self {
            title: String::from(format!("{}{}", String::from("🗖 "), String::from(title))),
            shape: df.shape(),
            data: df.clone(),
            summary_data: df.describe(None).unwrap_or_default(),
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
        }
    }

    fn show(&mut self, ctx: &egui::Context) {
        let window = Window::new(&self.title);

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
                        ui.radio_value(&mut self.filter.filter_action, FilterAction::New, "New");
                        ui.radio_value(
                            &mut self.filter.filter_action,
                            FilterAction::InPlace,
                            "In Place",
                        );
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
                            // TODO: Better handling of filtered dataframe
                            // TODO: Chained filtering
                            if f_df.is_ok() {
                                self.filter.filtered_data = f_df.ok();
                            } else {
                                self.data = self.data.clone()
                            };
                        }
                    })
                });
                ui.collapsing("Aggregate", |ui| {
                    ui.label(egui::RichText::new("Group by:").text_style(egui::TextStyle::Heading));
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
                    ui.label(format!("{:?}", &self.aggregate.groupby));
                    ui.label(egui::RichText::new("Columns:").text_style(egui::TextStyle::Heading));
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
                    ui.label(format!("{:?}", &self.aggregate.aggcols));
                    ui.label(egui::RichText::new("Metric:").text_style(egui::TextStyle::Heading));
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

                    if ui.button("Apply").clicked() {
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
                        if aggdf.is_ok() {
                            self.aggregate.aggdata = Some(aggdf.unwrap_or_default());
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
                ui.collapsing("Join", |ui| {});
                ui.collapsing("Melt", |ui| {
                    ui.label(egui::RichText::new("Id Vars:").text_style(egui::TextStyle::Heading));
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
                    ui.label(format!("{:?}", &self.melt.id_vars));
                    ui.label(
                        egui::RichText::new("Value Vars:").text_style(egui::TextStyle::Heading),
                    );
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
                    ui.label(format!("{:?}", &self.melt.value_vars));
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
                ui.collapsing("Concatenate", |ui| {});
            });
    }
}

impl eframe::App for TemplateApp {
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
                            if let Some(f) = &mut self.frames {
                                f.push(Rc::new(RefCell::new(DataFrameContainer::new(
                                    df, file_name,
                                ))))
                            }
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
            if let Some(frames_vec) = &mut self.frames.clone() {
                for frame_rc in frames_vec.iter() {
                    let mut frame_refcell = frame_rc.borrow_mut();
                    frame_refcell.show(ctx);

                    // Filter creates a new DataFrameContainer. InPlace option updates the
                    // existing container with the new one. The New option displays the filtered
                    // data in a new window.
                    // TODO: revise/re-factor filter functionality
                    if frame_refcell.filter.filtered_data.is_some() {
                        let filtered_title =
                            format!("filtered_{}{}", &frame_refcell.title, frames_vec.len());

                        let filtered_df = DataFrameContainer::new(
                            frame_refcell
                                .clone()
                                .filter
                                .filtered_data
                                .unwrap_or_default(),
                            &filtered_title,
                        );
                        match frame_refcell.filter.filter_action {
                            FilterAction::New => {
                                self.frames
                                    .as_mut()
                                    .unwrap()
                                    .push(Rc::new(RefCell::new(filtered_df)).to_owned());
                                // cleanup. set original filtered data back to None
                                frame_refcell.filter.filtered_data = None;
                            }
                            FilterAction::InPlace => {
                                frame_refcell.data = filtered_df.data.clone();
                                frame_refcell.shape = filtered_df.data.shape().clone();
                                frame_refcell.summary_data =
                                    filtered_df.data.clone().describe(None).unwrap_or_default();
                            }
                        }
                    }
                }
            }

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
    filter_action: FilterAction,
    filtered_data: Option<DataFrame>,
}

impl Default for DataFrameFilter {
    fn default() -> Self {
        Self {
            column: String::from(""),
            operation: FilterOps::EqualNum,
            value: String::from(""),
            filter_action: FilterAction::New,
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

#[derive(Debug, Clone, PartialEq)]
enum FilterAction {
    InPlace,
    New,
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
                                ui.label(format!("{}", value));
                            }
                        }
                    });
                }
            });
        });
}
