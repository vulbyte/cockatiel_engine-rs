//! Read-only SQL boundary tests: the engine's `DatabaseQuery` only permits
//! SELECT/WITH/EXPLAIN for modules; every write construct must be rejected.

use crate::is_read_only_sql;

#[test]
fn read_only_queries_pass() {
    assert!(is_read_only_sql("SELECT * FROM timeline_events"));
    assert!(is_read_only_sql("  SELECT uuid7 FROM users"));
    assert!(is_read_only_sql("WITH x AS (SELECT 1) SELECT * FROM x"));
    assert!(is_read_only_sql("EXPLAIN SELECT * FROM timeline_events"));
    // Case-insensitive keyword.
    assert!(is_read_only_sql("select 1"));
    assert!(is_read_only_sql("With x AS (select 1) select * from x"));
}

#[test]
fn comment_prefixes_do_not_bypass_the_gate() {
    assert!(is_read_only_sql("-- note\nSELECT 1"));
    assert!(is_read_only_sql("/* block */ SELECT 1"));
    // A comment followed by a write must still fail.
    assert!(!is_read_only_sql("-- note\nDROP TABLE timeline_events"));
    assert!(!is_read_only_sql("/* block */ DELETE FROM timeline_events"));
}

#[test]
fn write_statements_are_rejected() {
    assert!(!is_read_only_sql("INSERT INTO timeline_events (x) VALUES (1)"));
    assert!(!is_read_only_sql("UPDATE timeline_events SET x = 1"));
    assert!(!is_read_only_sql("DELETE FROM timeline_events"));
    assert!(!is_read_only_sql("DROP TABLE timeline_events"));
    assert!(!is_read_only_sql("PRAGMA wal_checkpoint"));
    assert!(!is_read_only_sql("CREATE TABLE x (y INT)"));
    assert!(!is_read_only_sql("ALTER TABLE x ADD y INT"));
}

/// SQL injection: a read-only prefix must not smuggle a write through.
#[test]
fn injected_write_via_semicolon_is_rejected() {
    assert!(!is_read_only_sql("SELECT 1; DROP TABLE timeline_events"));
    assert!(!is_read_only_sql("SELECT 1; DELETE FROM timeline_events"));
    assert!(!is_read_only_sql("SELECT 1; UPDATE timeline_events SET x=1"));
}

#[test]
fn empty_or_garbage_does_not_pass() {
    assert!(!is_read_only_sql(""));
    assert!(!is_read_only_sql("   "));
    assert!(!is_read_only_sql("SHOW TABLES"));
    assert!(!is_read_only_sql("VACUUM"));
}