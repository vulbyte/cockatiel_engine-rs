//! Read-only SQL boundary tests: the engine's `DatabaseQuery` only permits
//! SELECT/EXPLAIN for modules; every write construct must be rejected.

use crate::queries::is_read_only_sql;

#[test]
fn read_only_queries_pass() {
    assert!(is_read_only_sql("SELECT * FROM timeline_events"));
    assert!(is_read_only_sql("  SELECT uuid7 FROM users"));
    assert!(is_read_only_sql("EXPLAIN SELECT * FROM timeline_events"));
    // Case-insensitive keyword.
    assert!(is_read_only_sql("select 1"));
}

#[test]
fn with_cte_is_not_a_read_only_leader() {
    // A `WITH` block can smuggle a write via a CTE (`WITH x AS (...) DELETE…`),
    // so it is no longer a permitted leading keyword.
    assert!(!is_read_only_sql("WITH x AS (SELECT 1) SELECT * FROM x"));
    assert!(!is_read_only_sql("With x AS (select 1) select * from x"));
    assert!(!is_read_only_sql("WITH x AS (SELECT 1) DELETE FROM timeline_events"));
    assert!(!is_read_only_sql("WITH x AS (SELECT 1) INSERT INTO timeline_events (uuid7) VALUES (1)"));
    assert!(!is_read_only_sql("WITH x AS (SELECT 1) UPDATE timeline_events SET x=1"));
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
    // Defense-in-depth token scan: a write keyword nested anywhere in the
    // statement (here inside a subquery) is fatal even when the leading
    // keyword is SELECT and there is no `;`.
    assert!(!is_read_only_sql("SELECT * FROM (DELETE FROM timeline_events)"));
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
#[test]
fn write_keywords_inside_string_literals_are_data_not_statements() {
    // A write keyword used as a VALUE (or identifier) in a read-only query is
    // not a write — the token scan must be string-literal-aware.
    assert!(is_read_only_sql("SELECT * FROM users WHERE name = 'delete'"));
    assert!(is_read_only_sql("SELECT * FROM users WHERE status = 'drop' AND flag = 'create'"));
    assert!(is_read_only_sql("SELECT 'insert', 'update' FROM users"));
    assert!(is_read_only_sql("SELECT * FROM users WHERE \"delete\" = 1"));
}

#[test]
fn real_write_keywords_still_rejected_even_near_literals() {
    // The literal-awareness must not hide an actual write.
    assert!(!is_read_only_sql("SELECT * FROM users WHERE name = 'x' DELETE FROM users"));
    assert!(!is_read_only_sql("DELETE FROM users WHERE name = 'delete'"));
    assert!(!is_read_only_sql("UPDATE users SET name = 'update'"));
    assert!(!is_read_only_sql("WITH x AS (SELECT 1) DELETE FROM users WHERE name = 'x'"));
}
