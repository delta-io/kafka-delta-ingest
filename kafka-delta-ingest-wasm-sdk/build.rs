fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTFLAGS");

    if let Some(target_os) = std::env::var_os("CARGO_CFG_TARGET_OS") {
        if target_os != "wasi" {
            return;
        }
    }

    if let Some(rustflags) = std::env::var_os("CARGO_ENCODED_RUSTFLAGS") {
        for flag in rustflags.to_string_lossy().split('\x1f') {
            if flag.ends_with("wasi-exec-model=reactor") {
                println!("cargo:rustc-cfg=wasi_exec_model_reactor");
                return;
            }
        }
    }
}
