fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The Cockatiel protocol types come from the published `cockatiel-proto`
    // crate (git-rev pinned), not from a local proto compile.

    // User database protocol — engine-internal, NOT part of cockatiel-lib.
    println!("cargo:rerun-if-changed=../cockatiel_user_database-rs/cockatiel_proto/user_database.proto");
    prost_build::compile_protos(
        &["../cockatiel_user_database-rs/cockatiel_proto/user_database.proto"],
        &["../cockatiel_user_database-rs/cockatiel_proto"],
    )?;

    Ok(())
}