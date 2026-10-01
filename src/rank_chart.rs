//! The rank chart — the repo-root, streamer-editable mapping of 0-1 rank to a
//! display tier name.
//!
//! The chart lives at the repo root (`rank_chart.json`) so the engine, the TUI
//! and term-chat all read the SAME names. Tier order in the file is arbitrary;
//! the engine sorts by `min` ascending and a rank maps to the highest tier
//! whose `min <= rank`. Numbers are for logic (the 0-1 rank on the wire);
//! names are for display only.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

/// One tier entry from the chart: a display name + the 0-1 rank it starts at.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankTier {
    pub name: String,
    pub min: f32,
}

/// The parsed + sorted chart.
#[derive(Debug, Clone, Default)]
pub struct RankChart {
    /// Sorted ascending by `min`.
    tiers: Vec<RankTier>,
}

impl RankChart {
    /// Parse a chart JSON blob and sort tiers ascending by `min`.
    pub fn from_json(data: &str) -> Result<Self, String> {
        #[derive(Deserialize)]
        struct Chart {
            #[serde(default)]
            tiers: Vec<RankTier>,
        }
        let chart: Chart =
            serde_json::from_str(data).map_err(|e| format!("rank_chart.json parse error: {e}"))?;
        let mut tiers = chart.tiers;
        tiers.sort_by(|a, b| a.min.total_cmp(&b.min));
        Ok(Self { tiers })
    }

    /// The display name for a 0-1 rank: the highest tier whose `min <= rank`.
    /// Falls back to the lowest tier (or "—" when the chart is empty).
    pub fn name_for_rank(&self, rank: f32) -> &str {
        let mut name = "—";
        for t in &self.tiers {
            if rank >= t.min {
                name = &t.name;
            } else {
                break;
            }
        }
        name
    }
}

fn default_chart_path() -> PathBuf {
    // The engine runs with CWD = the engine submodule dir; the chart is one
    // level up at the repo root. `COCKATIEL_RANK_CHART` overrides it (the TUI
    // passes the absolute path so it works regardless of launch dir).
    if let Some(p) = std::env::var("COCKATIEL_RANK_CHART").ok().filter(|p| !p.is_empty()) {
        return PathBuf::from(p);
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.parent().unwrap_or(&cwd).join("rank_chart.json"),
        Err(_) => PathBuf::from("rank_chart.json"),
    }
}

static CHART: OnceLock<Arc<Mutex<RankChart>>> = OnceLock::new();

/// Load the rank chart (cached). A missing/unparseable chart falls back to
/// the default mineral template so display never breaks.
pub fn chart() -> Arc<Mutex<RankChart>> {
    CHART
        .get_or_init(|| {
            let default = default_chart();
            let loaded = std::fs::read_to_string(default_chart_path())
                .ok()
                .and_then(|s| RankChart::from_json(&s).ok())
                .unwrap_or(default);
            Arc::new(Mutex::new(loaded))
        })
        .clone()
}

/// The built-in mineral template, used when the root chart is missing.
pub fn default_chart() -> RankChart {
    RankChart::from_json(
        r#"{
  "tiers": [
    { "name": "coal",     "min": 0.0 },
    { "name": "copper",   "min": 0.1 },
    { "name": "bronze",   "min": 0.2 },
    { "name": "silver",   "min": 0.3 },
    { "name": "gold",     "min": 0.4 },
    { "name": "sapphire", "min": 0.5 },
    { "name": "emerald",  "min": 0.6 },
    { "name": "ruby",     "min": 0.7 },
    { "name": "diamond",  "min": 0.8 },
    { "name": "opal",     "min": 0.9 }
  ]
}"#,
    )
    .expect("built-in mineral template must parse")
}

/// Convenience: the tier name for a rank, resolving the shared chart.
pub fn tier_name(rank: f32) -> String {
    chart().lock().unwrap().name_for_rank(rank).to_string()
}

/// A map of known tier names -> display colors (for styling that wants a
/// color without loading CSS). Unknown/custom tier names fall back to White.
#[allow(dead_code)] // public helper; consumed by styling layers when wired up
pub fn tier_color(name: &str) -> String {
    let mut colors: HashMap<&str, &str> = HashMap::new();
    colors.insert("coal", "darkgray");
    colors.insert("copper", "orange");
    colors.insert("bronze", "yellow");
    colors.insert("silver", "silver");
    colors.insert("gold", "gold");
    colors.insert("sapphire", "blue");
    colors.insert("emerald", "green");
    colors.insert("ruby", "red");
    colors.insert("diamond", "cyan");
    colors.insert("opal", "lightcyan");
    colors.get(name).copied().unwrap_or("white").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chart_sorts_and_looks_up_highest_min() {
        // Deliberately unsorted in the file.
        let c = RankChart::from_json(
            r#"{"tiers":[
                {"name":"opal","min":0.9},
                {"name":"coal","min":0.0},
                {"name":"silver","min":0.3},
                {"name":"gold","min":0.4}
            ]}"#,
        )
        .unwrap();
        assert_eq!(c.name_for_rank(0.0), "coal");
        assert_eq!(c.name_for_rank(0.2), "coal");
        assert_eq!(c.name_for_rank(0.3), "silver");
        assert_eq!(c.name_for_rank(0.39), "silver");
        assert_eq!(c.name_for_rank(0.4), "gold");
        assert_eq!(c.name_for_rank(0.9), "opal");
        assert_eq!(c.name_for_rank(0.99), "opal");
    }

    #[test]
    fn default_template_is_full_mineral_ladder() {
        let c = default_chart();
        let names: Vec<String> = c.tiers.iter().map(|t| t.name.clone()).collect();
        assert_eq!(
            names,
            vec![
                "coal", "copper", "bronze", "silver", "gold", "sapphire",
                "emerald", "ruby", "diamond", "opal"
            ]
        );
        assert_eq!(c.tiers.iter().map(|t| t.min).collect::<Vec<f32>>(),
            vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]);
    }
}