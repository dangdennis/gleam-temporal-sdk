//// Downloads the prebuilt native library this package needs, so that using it takes no
//// Rust toolchain. Run it once after adding the package, and again after `gleam clean` or
//// a version change:
////
////     gleam run -m temporal/install
////
//// It picks the library published for this package version and this machine (Linux or
//// macOS, x86-64 or ARM). Pass a release tag, such as `v0.1.0`, to choose another version.

import gleam/io

/// Download the library, then stop with status 1 if that failed.
pub fn main() -> Nil {
  case install() {
    Ok(message) -> io.println(message)
    Error(message) -> {
      io.println_error(message)
      halt(1)
    }
  }
}

@external(erlang, "temporal_install_ffi", "install")
fn install() -> Result(String, String)

@external(erlang, "erlang", "halt")
fn halt(status: Int) -> Nil
