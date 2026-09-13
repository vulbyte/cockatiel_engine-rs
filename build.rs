fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Rerun build script if the proto file changes
    println!("cargo:rerun-if-changed=cockatiel_proto/cockatiel_protobuf.proto");

    prost_build::compile_protos(
        &["cockatiel_proto/cockatiel_protobuf.proto"],
        &["cockatiel_proto"],
    )?;

    Ok(())
}
