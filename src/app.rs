use egui::{ComboBox, TextEdit, Window};
use egui_extras::{Column, TableBuilder};
use polars::prelude::*;
use rfd::FileDialog;
use std::cell::RefCell;
use std::fmt::Debug;
use std::path::PathBuf;
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

#[derive(Clone, Debug)]
pub struct DataFrameContainer {
    title: String,
    shape: (usize, usize),
    data: DataFrame,
    summary_data: DataFrame,
    columns: Vec<String>,
    data_display: bool,
    is_open: bool,
    filter: DataFrameFilter,
    filter_action: FilterAction,
}

impl DataFrameContainer {
    fn new(file_path: PathBuf) -> Self {
        let df: DataFrame = CsvReader::from_path(&file_path).unwrap().finish().unwrap();
        Self {
            title: String::from(format!(
                "{}{}",
                String::from("🗖 "),
                String::from(file_path.file_name().unwrap().to_str().unwrap())
            )),
            shape: df.shape(),
            data: df.clone(),
            summary_data: df.describe(None).unwrap_or_default(),
            columns: df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect(),
            data_display: false,
            is_open: true,
            filter: DataFrameFilter::default(),
            filter_action: FilterAction::New,
        }
    }

    fn show(&mut self, ctx: &egui::Context) {
        let window = Window::new(&self.title);

        window
            .open(&mut self.is_open)
            .scroll2([true, true])
            .resize(|r| r.max_size((1920.0, 1080.0)))
            .resizable(true)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label("Shape: ");
                    ui.label(String::from(format!("{:?}", &self.shape)));
                });
                ui.horizontal(|ui| {
                    ui.label("Data: ");
                    let btn = ui.button("View");
                    if btn.clicked() {
                        self.data_display = !&self.data_display;
                    }
                });
                ui.collapsing("Columns", |ui| {
                    for c in &self.columns {
                        ui.label(c.to_owned());
                    }
                });
                ui.collapsing("Summary", |ui| {
                    let nr_cols = self.summary_data.width();
                    let nr_rows = self.summary_data.height();
                    let cols = &self.summary_data.get_column_names();

                    println!("{}", nr_cols);
                    println!("{}", nr_rows);
                    println!("{:?}", cols);

                    TableBuilder::new(ui)
                        .columns(Column::auto(), nr_cols)
                        .striped(true)
                        .resizable(true)
                        .header(5.0, |mut header| {
                            for head in cols {
                                header.col(|ui| {
                                    ui.heading(format!("{}", head));
                                });
                            }
                        })
                        .body(|body| {
                            body.rows(10.0, nr_rows, |row_index, mut row| {
                                for col in cols {
                                    row.col(|ui| {
                                        if let Ok(column) = &self.summary_data.column(col) {
                                            if let Ok(value) = column.get(row_index) {
                                                ui.label(format!("{}", value));
                                            }
                                        }
                                    });
                                }
                            });
                        });
                });
                ui.collapsing("Filter", |ui| {
                    ui.horizontal(|ui| {
                        ui.radio_value(&mut self.filter_action, FilterAction::New, "New");
                        ui.radio_value(&mut self.filter_action, FilterAction::InPlace, "In Place");
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
                        ComboBox::from_label("than")
                            .selected_text(format!("{:?}", &self.filter.operation))
                            .show_ui(ui, |ui| {
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOperations::EqualNum,
                                    "=",
                                );
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOperations::EqualStr,
                                    "= ''",
                                );
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOperations::GreaterThan,
                                    ">",
                                );
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOperations::GreaterEqualThan,
                                    ">=",
                                );
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOperations::LowerThan,
                                    "<",
                                );
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOperations::LowerEqualThan,
                                    "=<",
                                );
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOperations::IsNull,
                                    "null",
                                );
                                ui.selectable_value(
                                    &mut self.filter.operation,
                                    FilterOperations::IsNotNull,
                                    "not null",
                                );
                            });
                        ui.add(TextEdit::singleline(&mut self.filter.value));
                        if ui.button("Filter").clicked() {
                            let f_df = filter_dataframe(
                                self.data.clone(),
                                &self.filter.column.clone(),
                                &self.filter.operation.clone(),
                                &self.filter.value.clone(),
                            );
                            // TODO: Better handling of filtered dataframe
                            if f_df.is_ok() {
                                self.data = f_df.unwrap()
                            } else {
                                self.data = self.data.clone()
                            }
                        }
                    })
                });
            });
    }

    fn show_data(&mut self, ctx: &egui::Context) {
        let window = Window::new(format!("{}{}", String::from("Data: "), &self.title))
            .open(&mut self.data_display)
            .resize(|r| r.max_size((1920.0, 1080.0)))
            .resizable(true)
            .scroll2([true, true])
            .constrain(false)
            .collapsible(true);

        window.show(ctx, |ui| {
            let nr_cols = self.data.width();
            let nr_rows = self.data.height();
            let cols = &self.data.get_column_names();

            TableBuilder::new(ui)
                .column(Column::auto())
                .columns(Column::auto(), nr_cols)
                .striped(true)
                .resizable(true)
                .header(5.0, |mut header| {
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
                                if let Ok(column) = &self.data.column(col) {
                                    if let Ok(value) = column.get(row_index) {
                                        ui.label(format!("{}", value));
                                    }
                                }
                            });
                        }
                    });
                });
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
                            if let Some(f) = &mut self.frames {
                                f.push(Rc::new(RefCell::new(DataFrameContainer::new(path))))
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
            if let Some(frames_vec) = &self.frames {
                for frame_rc in frames_vec.iter() {
                    let mut frame_refcell = frame_rc.borrow_mut();
                    frame_refcell.show(ctx);
                    if frame_refcell.data_display {
                        frame_refcell.data_display = frame_refcell.data_display;
                        if frame_refcell.data_display {
                            frame_refcell.show_data(ctx)
                        }
                    }
                }
            }
            egui::warn_if_debug_build(ui);
        });
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataFrameFilter {
    column: String,
    operation: FilterOperations,
    value: String,
}

impl DataFrameFilter {
    fn new(&self, column: &str, operation: FilterOperations, value: &str) -> Self {
        Self {
            column: String::from(column),
            operation: operation,
            value: String::from(value),
        }
    }
}

impl Default for DataFrameFilter {
    fn default() -> Self {
        Self {
            column: String::from(""),
            operation: FilterOperations::EqualNum,
            value: String::from(""),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum FilterOperations {
    EqualNum,
    EqualStr,
    GreaterThan,
    GreaterEqualThan,
    LowerThan,
    LowerEqualThan,
    IsNull,
    IsNotNull,
}

fn filter_dataframe(
    df: DataFrame,
    column: &str,
    operation: &FilterOperations,
    value: &String,
) -> Result<DataFrame, PolarsError> {
    let dff = match operation {
        FilterOperations::EqualNum => df
            .lazy()
            .filter(col(column).eq(lit(value.parse::<f64>().unwrap_or_default())))
            .collect(),
        FilterOperations::EqualStr => df
            .lazy()
            .filter(col(column).eq(lit(value.parse::<String>().unwrap_or_default())))
            .collect(),
        FilterOperations::GreaterThan => df
            .lazy()
            .filter(col(column).gt(lit(value.parse::<f64>().unwrap_or_default())))
            .collect(),
        FilterOperations::GreaterEqualThan => df
            .lazy()
            .filter(col(column).gt_eq(lit(value.parse::<f64>().unwrap_or_default())))
            .collect(),
        FilterOperations::LowerThan => df
            .lazy()
            .filter(col(column).lt(lit(value.parse::<f64>().unwrap_or_default())))
            .collect(),
        FilterOperations::LowerEqualThan => df
            .lazy()
            .filter(col(column).lt_eq(lit(value.parse::<f64>().unwrap_or_default())))
            .collect(),
        FilterOperations::IsNull => df.lazy().filter(col(column).is_null()).collect(),
        FilterOperations::IsNotNull => df.lazy().filter(col(column).is_not_null()).collect(),
    };
    dff
}

#[derive(Debug, Clone, PartialEq)]
enum FilterAction {
    InPlace,
    New,
}
