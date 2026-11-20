//// Downloads the prebuilt native library this package needs, so that using it takes no
//// Rust toolchain. Run it once after adding the package, and again after `gleam clean` or
//// a version change:
////
////     gleam run -m temporal/install
////
//// It picks the library published for this package version and this machine: Linux with
//// glibc (Debian, Ubuntu and most others) or musl (Alpine), or macOS, on x86-64 or ARM.
//// Pass a release tag, such as `v0.3.0`, to choose another version.

import gleam/io
import gleam/result
import gleam/string

/// Download the library, then stop with status 1 if that failed.
pub fn main() -> Nil {
  let #(architecture, os, alpine) = machine()
  case target(architecture:, os:, alpine:) |> result.try(install) {
    Ok(message) -> io.println(message)
    Error(message) -> {
      io.println_error(message)
      halt(1)
    }
  }
}

/// The Rust target whose library a machine needs. `architecture` is what Erlang reports
/// as its `system_architecture`, such as `x86_64-pc-linux-musl`, `os` the name in
/// `os:type()`, and `alpine` whether `/etc/alpine-release` exists.
///
/// A library built for one C library cannot be loaded by a VM built for the other, so on
/// Linux this looks for musl: in the architecture, where the VMs of the `erlang:alpine`
/// and `hexpm/erlang` Alpine images and of Alpine's own package all name it, or failing
/// that in the system being Alpine.
@internal
pub fn target(
  architecture architecture: String,
  os os: String,
  alpine alpine: Bool,
) -> Result(String, String) {
  let cpu = case architecture {
    "x86_64" <> _ | "amd64" <> _ -> Ok("x86_64")
    "aarch64" <> _ | "arm64" <> _ -> Ok("aarch64")
    _ -> Error(Nil)
  }
  let musl = alpine || string.contains(architecture, "musl")
  case cpu, os {
    Ok(cpu), "linux" if musl -> Ok(cpu <> "-unknown-linux-musl")
    Ok(cpu), "linux" -> Ok(cpu <> "-unknown-linux-gnu")
    Ok(cpu), "darwin" -> Ok(cpu <> "-apple-darwin")
    _, _ ->
      Error(
        "there is no prebuilt library for "
        <> architecture
        <> " ("
        <> os
        <> "): they are built for Linux, with glibc or musl, and for macOS, on x86-64 "
        <> "and ARM. Compile it with bin/build-nif in the package directory",
      )
  }
}

/// This machine: Erlang's system architecture, the operating system's name, and whether
/// it is Alpine Linux.
@external(erlang, "temporal_install_ffi", "machine")
fn machine() -> #(String, String, Bool)

@external(erlang, "temporal_install_ffi", "install")
fn install(target: String) -> Result(String, String)

@external(erlang, "erlang", "halt")
fn halt(status: Int) -> Nil
