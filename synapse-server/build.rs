use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../proto");

    let protos = &[
        proto_root.join("synapse/v1/memory.proto"),
        proto_root.join("synapse/v1/conflict.proto"),
        proto_root.join("synapse/v1/cluster.proto"),
    ];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(protos, &[&proto_root])?;

    // Re-run if protos change
    for proto in protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    Ok(())
}
