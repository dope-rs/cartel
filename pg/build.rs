fn main() {
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    if arch != "aarch64" && arch != "arm64" {
        panic!("cartel-pg supports only ARM64/AArch64 targets; got target_arch={arch}",);
    }
}
