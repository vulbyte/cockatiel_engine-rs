//! Engine unit tests — kept out of main.rs so the binary stays readable.
//!
//! Submodules are per-area so new tests are easy to add without touching
//! main.rs: drop a file in here, declare it below, and write the tests.

mod auth_tests;
mod command_tests;
mod sql_tests;