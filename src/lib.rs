#![warn(clippy::all, rust_2018_idioms)]

mod aggregate;
mod app;
mod container;
mod filter;
mod join;
mod melt;
mod summary;
mod utils;
pub use app::App;
