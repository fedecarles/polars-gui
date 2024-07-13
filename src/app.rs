use crate::container::*;
use polars::prelude::*;
#[cfg(not(target_arch = "wasm32"))]
use rfd::FileDialog;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;

#[cfg(target_arch = "wasm32")]
use rfd::AsyncFileDialog;
#[cfg(target_arch = "wasm32")]
use std::future::Future;

/// We derive Deserialize/Serialize so we can persist app state on shutdown.
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
#[serde(default)] // if we add new fields, give them default values when deserializing old state
pub struct App {
    label: String,
    // this how you opt-out of serialization of a member
    #[serde(skip)]
    version: f32,
    #[serde(skip)]
    frames: Rc<RefCell<Vec<HashMap<String, DataFrameContainer>>>>,
    titles: Rc<RefCell<Vec<String>>>,
    df_cols: Rc<RefCell<HashMap<String, Vec<String>>>>,
}

impl Default for App {
    fn default() -> Self {
        Self {
            label: "Polars GUI".to_owned(),
            version: 0.1,
            frames: Rc::new(RefCell::new(Vec::new())),
            titles: Rc::new(RefCell::new(Vec::new())),
            df_cols: Rc::new(RefCell::new(HashMap::default())),
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
        if let Some(storage) = cc.storage {
            return eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default();
        }
        Default::default()
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
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            // The top panel is often a good place for a menu bar:
            egui::menu::bar(ui, |ui| {
                ui.menu_button("New", |ui| {
                    if ui.button("DataFrame").clicked() {
                        #[cfg(target_arch = "wasm32")]
                        {
                            let frames = Rc::clone(&self.frames);
                            let titles = Rc::clone(&self.titles);
                            let df_cols = Rc::clone(&self.df_cols);

                            execute(async move {
                                let file = AsyncFileDialog::new().pick_file().await;

                                if let Some(file) = file {
                                    //file.read().await;
                                    let content = file.read().await;
                                    let cursor = std::io::Cursor::new(content);
                                    let df = CsvReader::new(cursor).finish().unwrap();
                                    let file_name = file.file_name();
                                    let mut hash = HashMap::new();
                                    hash.insert(
                                        file_name.to_string(),
                                        DataFrameContainer::new(df.clone(), &file_name),
                                    );
                                    frames.borrow_mut().push(hash);
                                    titles.borrow_mut().push(file_name.to_string());
                                    let cols = df
                                        .clone()
                                        .get_column_names()
                                        .iter()
                                        .map(|c| c.to_string())
                                        .collect();
                                    df_cols.borrow_mut().insert(String::from(file_name), cols);
                                }
                            });
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(path) = FileDialog::new().pick_file() {
                            let df: DataFrame = CsvReadOptions::default()
                                .with_has_header(true)
                                .with_infer_schema_length(Some(10000))
                                .try_into_reader_with_file_path(Some(path.clone()))
                                .unwrap()
                                .finish()
                                .unwrap();
                            let file_name: &str = &path.file_name().unwrap().to_str().unwrap();
                            let mut hash = HashMap::new();
                            hash.insert(
                                file_name.to_string(),
                                DataFrameContainer::new(df.clone(), file_name),
                            );
                            self.frames.borrow_mut().push(hash);
                            let cols = df
                                .clone()
                                .get_column_names()
                                .iter()
                                .map(|c| c.to_string())
                                .collect();
                            self.df_cols
                                .borrow_mut()
                                .insert(String::from(file_name), cols);
                            self.titles.borrow_mut().push(file_name.to_string());
                        }
                    }
                });
                ui.menu_button("App", |ui| {
                    if ui.button("Quit").clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
            });
        });

        egui::CentralPanel::default().show(ctx, |_ui| {
            let mut temp_frames = Vec::new(); // Temporary vector to hold the filtered frames
            let temp_joins = &self.frames.borrow_mut().clone();
            let nr_frames = &self.frames.borrow_mut().len();

            for map in self.frames.borrow_mut().iter_mut() {
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
                            }
                        }
                    }

                    // Join requires the selection of another DataFrameContainer in the frames list
                    // and the mapped columns stored in df_cols.
                    frame_refcell.join.df_list = self.titles.borrow_mut().clone();
                    let cols = self.df_cols.borrow_mut().clone();
                    let df_cols = cols.get(&frame_refcell.join.df_selection);

                    if df_cols.is_some() {
                        frame_refcell.join.right_on_cols =
                            df_cols.unwrap_or(&Vec::new()).to_owned();
                    }

                    if frame_refcell.join.join {
                        frame_refcell.clone().join_dataframe(
                            frame_refcell,
                            &mut temp_frames,
                            temp_joins,
                        );
                    }
                }
            }
            // Push the filtered frames into self.frames after the nested loops
            self.frames.borrow_mut().extend(temp_frames);
        });
    }
}

#[cfg(target_arch = "wasm32")]
fn execute<F: Future<Output = ()> + 'static>(f: F) {
    wasm_bindgen_futures::spawn_local(f);
}
