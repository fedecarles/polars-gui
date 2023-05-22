use egui::Grid;
use egui::Label;
use egui::Slider;
use egui::Window;
use polars::prelude::*;
use rfd::FileDialog;
use std::cell::RefCell;
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
    name: String,
    shape: String,
    data: DataFrame,
    table_data: Vec<Vec<String>>,
    columns: Vec<String>,
    data_display: bool,
}

impl DataFrameContainer {
    fn new(file_path: PathBuf) -> Self {
        let df: DataFrame = CsvReader::from_path(&file_path).unwrap().finish().unwrap();
        Self {
            name: String::from(format!(
                "{}{}",
                String::from("🗖 "),
                String::from(file_path.file_name().unwrap().to_str().unwrap())
            )),
            shape: String::from(format!("{:?}", df.shape())),
            data: df.clone(),
            table_data: Vec::new(),
            columns: df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect(),
            data_display: false,
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Shape: ");
            ui.label(self.shape.to_owned());
        });
        ui.horizontal(|ui| {
            ui.label("Data: ");
            let btn = ui.button("View");
            if btn.clicked() {
                self.table_data = df_to_vec(&self.data);
                self.data_display = !self.data_display;
            }
        });
        ui.collapsing("Columns", |ui| {
            for c in &self.columns {
                ui.label(c.to_owned());
            }
        });
    }

    fn show(&mut self, ctx: &egui::Context) {
        let window = Window::new(&self.name);
        window.show(ctx, |ui| {
            self.ui(ui);
        });
    }

    fn data_ui(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Rows: ");
            ui.add(Slider::new(&mut 0, 0..=1000));
        });

        Grid::new(&self.name)
            .num_columns(self.table_data[0].len())
            .striped(true)
            .show(ui, |ui| {
                for row in &self.table_data {
                    for cell in row {
                        ui.add(Label::new(cell));
                    }
                    ui.end_row()
                }
            });
    }

    fn show_data(&mut self, ctx: &egui::Context) {
        let window = Window::new(format!("{}{}", String::from("Data: "), &self.name))
            .fixed_size((300.0, 300.0))
            .resize(|r| r.max_size((700.0, 700.0)))
            .resizable(true)
            .scroll2([true, true])
            .constrain(false)
            .collapsible(true);
        window.show(ctx, |ui| {
            self.data_ui(ui);
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

fn df_to_vec(df: &DataFrame) -> Vec<Vec<String>> {
    let mut string_vectors: Vec<Vec<String>> = Vec::new();

    let mut headers: Vec<String> = df
        .get_column_names()
        .to_vec()
        .iter()
        .map(|h| h.to_string())
        .collect();
    headers.insert(0, String::from("index"));
    string_vectors.push(headers);

    //FIXME This works but is very slow.
    for row in 0..df.height() {
        let df_row = df.get(row);
        let r = df_row.unwrap_or_default().to_vec();
        let mut str_vec: Vec<String> = r.iter().map(|x| x.to_string()).collect();
        str_vec.insert(0, row.to_string());
        string_vectors.push(str_vec)
    }
    string_vectors
}
