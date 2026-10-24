//! Engine unit tests — kept out of main.rs so the binary stays readable.
//!
//! Submodules are per-area so new tests are easy to add without touching
//! main.rs: drop a file in here, declare it below, and write the tests.

mod ack_tests;
mod auth_tests;
mod command_tests;
mod config_tests;
mod db_tests;
mod e2e_pipeline_tests;
mod handshake_tests;
mod ingest_fetch_tests;
mod integrity_tests;
mod pause_tests;
mod pin_gate_tests;
mod pipeline_tests;
mod prediction_tests;
mod shutdown_tests;
mod sql_tests;
