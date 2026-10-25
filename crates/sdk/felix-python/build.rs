// An extension module leaves the Python symbols to the interpreter that loads
// it. Linux allows that by default; macOS needs `-undefined dynamic_lookup`,
// which maturin passes but a plain `cargo build` does not.
fn main() {
    pyo3_build_config::add_extension_module_link_args();
}
