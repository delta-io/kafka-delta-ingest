#[cfg(not(any(feature = "s3", feature = "azure")))]
compile_error!(
    "Either the \"s3\" or the \"azure\" feature must be built to compile kafka-delta-ingest"
);
fn main() {}
