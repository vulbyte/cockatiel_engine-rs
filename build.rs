use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // All Cockatiel protocol types — the shared wire protocol AND the
    // user-database model — come from the published `cockatiel-proto` crate
    // (git-rev pinned), not from a local proto compile.
    //
    // We also stamp the build commit into the binary so the engine's actor id
    // (`actor_id::cockatiel_actor_id`) names the exact revision that issued a
    // commendation/reprimand. `unknown` keeps the build alive outside a git
    // checkout.
    let git = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|hash| !hash.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=COCKATIEL_GIT_HASH={git}");
    Ok(())
}
