//// Downloads the prebuilt native library this package needs, so that using it takes no
//// Rust toolchain. Run it once after adding the package, and again after `gleam clean` or
//// a version change:
////
////     gleam run -m temporal/install
////
//// It picks the library published for this package version and this machine: Linux with
//// glibc (Debian, Ubuntu and most others) or musl (Alpine), or macOS, on x86-64 or ARM.
////
//// The download is checked before it is used. A release of this package carries, in
//// `priv/checksums.txt`, the SHA-256 of each of its libraries, and the installer puts a
//// library in place only if what it downloaded has the digest written there. So the
//// library that gets loaded into your VM is the one the release was made with, whatever
//// the download server or the network in between sent. Anything else is refused: a
//// download that does not match, and a copy of the package whose checksums are not for
//// its own version, such as a checkout of the repository between releases. There is no
//// way to install another version's library; to use this package from source, compile
//// the library with `bin/build-nif` in the package directory.

import gleam/dict.{type Dict}
import gleam/int
import gleam/io
import gleam/list
import gleam/result
import gleam/string

const releases =
  "https://github.com/dangdennis/gleam-temporal-sdk/releases/download/"

const build_it =
  "Compile the library with bin/build-nif in the package directory"

/// Download and verify the library, then stop with status 1 if that failed.
pub fn main() -> Nil {
  let #(architecture, os, alpine) = machine()
  let installed = {
    use target <- result.try(target(architecture:, os:, alpine:))
    use checksums <- result.try(read_checksums())
    install(
      checksums:,
      version: version(),
      target:,
      into: library_path(),
      fetch: download,
    )
  }
  case installed {
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

/// What `priv/checksums.txt` says: the release it was written for, the commit that
/// release's libraries were built from, and the SHA-256 of each library, in lowercase
/// hexadecimal, by file name.
@internal
pub type Checksums {
  Checksums(version: String, commit: String, digests: Dict(String, String))
}

/// Read a checksums file: a first line `# v<version> <commit>`, then the lines
/// `sha256sum` prints, `<64 hex digits>  <file name>`. Blank lines and further comments
/// are skipped.
///
/// A file with no such first line, as the repository has between releases, is
/// `Error(NotARelease)`. A line that is neither a comment nor a checksum is
/// `Error(Malformed(..))`: a file that cannot be read in full is not trusted in part.
@internal
pub fn parse_checksums(text: String) -> Result(Checksums, ChecksumsError) {
  let lines =
    string.split(text, "\n")
    |> list.map(string.trim)
    |> list.index_map(fn(line, index) { #(index + 1, line) })
    |> list.filter(fn(line) { line.1 != "" })
  case lines {
    [#(_, "# v" <> header), ..rest] ->
      case string.split(header, " ") |> list.filter(fn(word) { word != "" }) {
        [version, commit] -> {
          use digests <- result.map(
            list.filter(rest, fn(line) { !string.starts_with(line.1, "#") })
            |> list.try_map(checksum_line),
          )
          Checksums(version:, commit:, digests: dict.from_list(digests))
        }
        _ -> Error(Malformed(1, "expected `# v<version> <commit>`"))
      }
    _ -> Error(NotARelease)
  }
}

/// Why a checksums file names no digests to trust.
@internal
pub type ChecksumsError {
  /// It has no `# v<version> <commit>` first line: no release wrote it.
  NotARelease
  Malformed(line: Int, problem: String)
}

/// One line of `sha256sum` output as `#(file name, digest)`. The digest and the name are
/// separated by two spaces, or by a space and `*` for a file read in binary mode.
fn checksum_line(
  line: #(Int, String),
) -> Result(#(String, String), ChecksumsError) {
  let #(number, text) = line
  let digest = string.lowercase(string.slice(text, 0, 64))
  let is_hex =
    string.length(digest) == 64
    && list.all(string.to_graphemes(digest), string.contains(
      "0123456789abcdef",
      _,
    ))
  case is_hex, string.drop_start(text, 64) {
    True, "  " <> name | True, " *" <> name if name != "" -> Ok(#(name, digest))
    False, _ -> Error(Malformed(number, "expected 64 hexadecimal digits"))
    True, _ ->
      Error(Malformed(
        number,
        "expected two spaces and a file name after the digest",
      ))
  }
}

/// The SHA-256 the library for `target` must have, from the text of
/// `priv/checksums.txt`, or why nothing may be installed: the checksums are not for this
/// `version` of the package, or name no library for the target.
@internal
pub fn expected_digest(
  checksums checksums: String,
  version version: String,
  target target: String,
) -> Result(String, String) {
  let not_a_release = fn(found) {
    Error(
      "this copy of temporal_gleam is not a release, so there is no prebuilt library "
      <> "that is known to match it: it is version "
      <> version
      <> " and its priv/checksums.txt "
      <> found
      <> ". "
      <> build_it,
    )
  }
  case parse_checksums(checksums) {
    Error(NotARelease) -> not_a_release("names no release")
    Error(Malformed(line, problem)) ->
      Error(
        "priv/checksums.txt cannot be read, so no download can be verified: line "
        <> int.to_string(line)
        <> ": "
        <> problem
        <> ". "
        <> build_it,
      )
    Ok(Checksums(version: released, ..)) if released != version ->
      not_a_release("is for version " <> released)
    Ok(Checksums(digests:, ..)) ->
      dict.get(digests, library(target))
      |> result.replace_error(
        "release v"
        <> version
        <> " has no library for this machine ("
        <> target
        <> "): priv/checksums.txt lists none. "
        <> build_it,
      )
  }
}

/// Whether a download has the digest the release recorded. Compared in full.
@internal
pub fn verify_digest(
  expected expected: String,
  actual actual: String,
) -> Result(Nil, String) {
  case string.lowercase(expected) == string.lowercase(actual) {
    True -> Ok(Nil)
    False ->
      Error(
        "the downloaded library is not the one this release was made with, and was "
        <> "not installed.\n  expected SHA-256 "
        <> expected
        <> "\n  downloaded      "
        <> actual
        <> "\nThe download was cut short or altered on the way. Try again; if the "
        <> "digests still differ, do not use the file. "
        <> build_it,
      )
  }
}

/// A library's file name in a release.
fn library(target: String) -> String {
  "libtemporal_nif-" <> target <> ".so"
}

/// Install the library for `target` at `into`: `fetch` downloads a URL to a path, and the
/// file is moved into place only once its SHA-256 is the one `checksums` has for it.
/// Whatever fails, nothing unverified is left at `into`, and the partial download is
/// deleted.
@internal
pub fn install(
  checksums checksums: String,
  version version: String,
  target target: String,
  into destination: String,
  fetch fetch: fn(String, String) -> Result(Nil, String),
) -> Result(String, String) {
  use expected <- result.try(expected_digest(checksums:, version:, target:))
  let url = releases <> "v" <> version <> "/" <> library(target)
  let partial = destination <> ".part"
  let verified = {
    use Nil <- result.try(
      fetch(url, partial)
      |> result.map_error(fn(reason) {
        "could not download "
        <> url
        <> "\n"
        <> string.trim_end(reason)
        <> "\n"
        <> build_it
      }),
    )
    use actual <- result.try(sha256(partial))
    use Nil <- result.try(verify_digest(expected:, actual:))
    move(partial, destination)
  }
  case verified {
    Ok(Nil) ->
      Ok(
        "installed the Temporal library v"
        <> version
        <> " for "
        <> target
        <> " (SHA-256 "
        <> expected
        <> ", verified)",
      )
    Error(reason) -> {
      delete(partial)
      Error(reason)
    }
  }
}

/// This machine: Erlang's system architecture, the operating system's name, and whether
/// it is Alpine Linux.
@external(erlang, "temporal_install_ffi", "machine")
fn machine() -> #(String, String, Bool)

/// This package's version, as in `gleam.toml`.
@external(erlang, "temporal_install_ffi", "version")
fn version() -> String

/// Where the NIF loader looks for the library.
@external(erlang, "temporal_install_ffi", "library_path")
fn library_path() -> String

/// The text of the package's `priv/checksums.txt`.
@external(erlang, "temporal_install_ffi", "read_checksums")
fn read_checksums() -> Result(String, String)

/// Download a URL to a file with curl.
@external(erlang, "temporal_install_ffi", "download")
fn download(url: String, path: String) -> Result(Nil, String)

/// The SHA-256 of a file, in lowercase hexadecimal.
@external(erlang, "temporal_install_ffi", "sha256")
fn sha256(path: String) -> Result(String, String)

@external(erlang, "temporal_install_ffi", "move")
fn move(from: String, to: String) -> Result(Nil, String)

@external(erlang, "temporal_install_ffi", "delete")
fn delete(path: String) -> Nil

@external(erlang, "erlang", "halt")
fn halt(status: Int) -> Nil
