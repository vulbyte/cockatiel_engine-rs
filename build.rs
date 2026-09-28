fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The Cockatiel protocol types come from the published `cockatiel-proto`
    // crate (git-rev pinned), not from a local proto compile.

    // User database protocol — engine-internal, NOT part of cockatiel-lib.
    println!("cargo:rerun-if-changed=../cockatiel_user_database-rs/cockatiel_proto/user_database.proto");
    prost_build::compile_protos(
        &["../cockatiel_user_database-rs/cockatiel_proto/user_database.proto"],
        &["../cockatiel_user_database-rs/cockatiel_proto"],
    )?;

    // Engine module protocol — engine-owned home for the timeline /
    // user-database query surface. Also engine-internal, NOT part of
    // cockatiel-lib.
    println!("cargo:rerun-if-changed=src/proto/engine_module.proto");
    prost_build::compile_protos(
        &["src/proto/engine_module.proto"],
        &["src/proto"],
    )?;

    Ok(())
}