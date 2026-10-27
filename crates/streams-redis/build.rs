fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=protos");
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(&["protos/temporal/sdk/streams/v1/redis.proto"], &["protos"])?;
    Ok(())
}
