use crate::container::*;
use egui_extras::{Column, TableBuilder};
use polars::prelude::*;
use std::collections::HashMap;

pub fn display_dataframe(df: &DataFrame, ui: &mut egui::Ui) {
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

pub fn get_container<'a>(
    containers: &'a Vec<HashMap<String, DataFrameContainer>>,
    title: &'a str,
) -> Option<DataFrameContainer> {
    for map in containers {
        if let Some(container) = map.get(title) {
            return Some(container.clone());
        }
    }
    None
}
