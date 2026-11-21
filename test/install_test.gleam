//// Which prebuilt library `temporal/install` picks for a machine, from what the machine
//// reports, and what it takes for a download to be installed: the checksum the release
//// recorded for it. The architecture strings are the ones real VMs print.

import gleam/bit_array
import gleam/dict
import gleam/int
import gleam/list
import gleam/string
import support
import temporal/install

pub fn glibc_linux_gets_the_gnu_library_test() {
  // Debian and Ubuntu packages, the `erlang` Docker image, asdf and kerl builds.
  assert install.target("x86_64-pc-linux-gnu", "linux", False)
    == Ok("x86_64-unknown-linux-gnu")
  assert install.target("x86_64-unknown-linux-gnu", "linux", False)
    == Ok("x86_64-unknown-linux-gnu")
  assert install.target("aarch64-unknown-linux-gnu", "linux", False)
    == Ok("aarch64-unknown-linux-gnu")
}

pub fn alpine_linux_gets_the_musl_library_test() {
  // The `erlang:alpine` and `hexpm/erlang` Alpine images.
  assert install.target("x86_64-pc-linux-musl", "linux", True)
    == Ok("x86_64-unknown-linux-musl")
  assert install.target("aarch64-unknown-linux-musl", "linux", True)
    == Ok("aarch64-unknown-linux-musl")
  // Alpine's own `erlang` package.
  assert install.target("x86_64-alpine-linux-musl", "linux", True)
    == Ok("x86_64-unknown-linux-musl")
  assert install.target("aarch64-alpine-linux-musl", "linux", True)
    == Ok("aarch64-unknown-linux-musl")
}

pub fn either_sign_of_musl_is_enough_test() {
  // A musl VM on a system that is not Alpine, such as Void Linux.
  assert install.target("x86_64-pc-linux-musl", "linux", False)
    == Ok("x86_64-unknown-linux-musl")
  // A VM on Alpine whose architecture does not name its C library.
  assert install.target("aarch64-unknown-linux", "linux", True)
    == Ok("aarch64-unknown-linux-musl")
}

pub fn macos_gets_the_darwin_library_test() {
  assert install.target("aarch64-apple-darwin24.1.0", "darwin", False)
    == Ok("aarch64-apple-darwin")
  assert install.target("x86_64-apple-darwin23.6.0", "darwin", False)
    == Ok("x86_64-apple-darwin")
  assert install.target("arm64-apple-darwin", "darwin", False)
    == Ok("aarch64-apple-darwin")
}

pub fn a_machine_with_no_library_is_told_how_to_build_one_test() {
  let assert Error(message) =
    install.target("amd64-portbld-freebsd14.1", "freebsd", False)
  assert string.contains(message, "amd64-portbld-freebsd14.1 (freebsd)")
  assert string.contains(message, "bin/build-nif")
  let assert Error(_) =
    install.target("armv7l-unknown-linux-gnueabihf", "linux", False)
  let assert Error(_) =
    install.target("riscv64-alpine-linux-musl", "linux", True)
  let assert Error(_) = install.target("win32", "nt", False)
}

// ---- the checksums a download is verified against ----

const gnu = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

const musl = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

/// A checksums file as the release workflow writes it.
fn released() -> String {
  "# v1.2.3 4f0c2a9e7d\n"
  <> gnu
  <> "  libtemporal_nif-x86_64-unknown-linux-gnu.so\n"
  <> musl
  <> "  libtemporal_nif-x86_64-unknown-linux-musl.so\n"
}

pub fn a_release_checksums_file_is_read_test() {
  let assert Ok(install.Checksums(version:, commit:, digests:)) =
    install.parse_checksums(released())
  assert version == "1.2.3"
  assert commit == "4f0c2a9e7d"
  assert dict.to_list(digests)
    |> list.sort(fn(a, b) { string.compare(a.0, b.0) })
    == [
      #("libtemporal_nif-x86_64-unknown-linux-gnu.so", gnu),
      #("libtemporal_nif-x86_64-unknown-linux-musl.so", musl),
    ]
  // Windows line endings, blank lines, later comments, upper case digits and
  // sha256sum's binary-mode marker are all read the same.
  let assert Ok(lenient) =
    install.parse_checksums(
      "# v1.2.3 4f0c2a9e7d\r\n\r\n# built by the release workflow\r\n"
      <> string.uppercase(gnu)
      <> " *libtemporal_nif-x86_64-unknown-linux-gnu.so\r\n",
    )
  assert lenient.digests
    == dict.from_list([#("libtemporal_nif-x86_64-unknown-linux-gnu.so", gnu)])
  assert install.expected_digest(
      checksums: released(),
      version: "1.2.3",
      target: "x86_64-unknown-linux-musl",
    )
    == Ok(musl)
}

pub fn a_file_no_release_wrote_installs_nothing_test() {
  // What the repository carries between releases is a comment and no more.
  let placeholder = "# Filled in by the Release workflow.\n"
  list.each(
    [placeholder, "", "\n", "# nothing yet\n" <> gnu <> "  a.so\n"],
    fn(text) {
      assert install.parse_checksums(text) == Error(install.NotARelease)
      let assert Error(message) =
        install.expected_digest(
          checksums: text,
          version: "1.2.3",
          target: "x86_64-unknown-linux-gnu",
        )
      assert string.contains(message, "not a release")
      assert string.contains(message, "bin/build-nif")
    },
  )
}

/// The file in the repository is the placeholder, or, on the commit a release is tagged
/// at, the checksums of that release: for this version, one per published target.
pub fn the_committed_checksums_file_is_a_placeholder_or_this_release_test() {
  let assert Ok(committed) = support.read_file("priv/checksums.txt")
  let assert Ok(text) = bit_array.to_string(committed)
  case install.parse_checksums(text) {
    Error(install.NotARelease) -> Nil
    Error(install.Malformed(..)) -> panic as "priv/checksums.txt is malformed"
    Ok(install.Checksums(version:, digests:, ..)) -> {
      let assert Ok(manifest) = support.read_file("gleam.toml")
      let assert Ok(manifest) = bit_array.to_string(manifest)
      assert string.contains(manifest, "\nversion = \"" <> version <> "\"\n")
      list.each(["x86_64", "aarch64"], fn(cpu) {
        list.each(
          ["unknown-linux-gnu", "unknown-linux-musl", "apple-darwin"],
          fn(system) {
            let assert Ok(_) =
              dict.get(
                digests,
                "libtemporal_nif-" <> cpu <> "-" <> system <> ".so",
              )
          },
        )
      })
    }
  }
}

pub fn checksums_for_another_version_install_nothing_test() {
  // A library from another version is never installed, even one that is listed.
  list.each(["1.2.4", "1.2", "0.3.0", "v1.2.3"], fn(version) {
    let assert Error(message) =
      install.expected_digest(
        checksums: released(),
        version:,
        target: "x86_64-unknown-linux-gnu",
      )
    assert string.contains(message, "not a release")
    assert string.contains(message, "is for version 1.2.3")
    assert string.contains(message, "version " <> version)
    assert string.contains(message, "bin/build-nif")
  })
}

pub fn a_release_without_a_library_for_the_machine_installs_nothing_test() {
  let assert Error(message) =
    install.expected_digest(
      checksums: released(),
      version: "1.2.3",
      target: "aarch64-apple-darwin",
    )
  assert string.contains(message, "release v1.2.3 has no library")
  assert string.contains(message, "aarch64-apple-darwin")
  assert string.contains(message, "bin/build-nif")
}

pub fn a_malformed_checksums_file_installs_nothing_test() {
  let header = "# v1.2.3 4f0c2a9e7d\n"
  let name = "  libtemporal_nif-x86_64-unknown-linux-gnu.so\n"
  let malformed = [
    // A short digest, one that is not hexadecimal, a single space, no name, no digest.
    #(header <> string.drop_end(gnu, 1) <> name, 2),
    #(header <> "g" <> string.drop_end(gnu, 1) <> name, 2),
    #(header <> gnu <> " libtemporal_nif-x86_64-unknown-linux-gnu.so\n", 2),
    #(header <> gnu <> "\n", 2),
    #(header <> musl <> name <> "libtemporal_nif-aarch64-apple-darwin.so\n", 3),
    // A header without a commit, or with more than one.
    #("# v1.2.3\n" <> gnu <> name, 1),
    #("# v1.2.3 4f0c 2a9e\n" <> gnu <> name, 1),
  ]
  list.each(malformed, fn(case_) {
    let assert Error(install.Malformed(line:, ..)) =
      install.parse_checksums(case_.0)
    assert line == case_.1
    // Even when the target's own line is intact, as in the fifth.
    let assert Error(message) =
      install.expected_digest(
        checksums: case_.0,
        version: "1.2.3",
        target: "x86_64-unknown-linux-gnu",
      )
    assert string.contains(message, "priv/checksums.txt cannot be read")
    assert string.contains(message, "line " <> int.to_string(case_.1))
    assert string.contains(message, "bin/build-nif")
  })
}

pub fn a_digest_must_match_in_full_test() {
  assert install.verify_digest(expected: gnu, actual: gnu) == Ok(Nil)
  assert install.verify_digest(expected: gnu, actual: string.uppercase(gnu))
    == Ok(Nil)
  let nearly = string.drop_end(gnu, 1) <> "b"
  list.each([musl, nearly, string.drop_end(gnu, 1), "", gnu <> "a"], fn(actual) {
    let assert Error(message) = install.verify_digest(expected: gnu, actual:)
    // Both digests are shown, for whoever has to find out which side is wrong.
    assert string.contains(message, "expected SHA-256 " <> gnu)
    assert string.contains(message, "downloaded      " <> actual)
    assert string.contains(message, "not installed")
  })
}

// ---- installing, with a download that needs no network ----

/// The SHA-256 of the three bytes `abc`.
const abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

/// A download that writes `bytes` wherever it is asked to, and records the URL.
fn serving(bytes: BitArray, key: String) {
  fn(url: String, path: String) {
    support.record(key, url)
    support.write_file(path, bytes)
    Ok(Nil)
  }
}

fn scratch() -> String {
  "build/install-test/" <> support.unique() <> "/libtemporal_nif.so"
}

fn checksums_for(digest: String) -> String {
  "# v1.2.3 4f0c2a9e7d\n"
  <> digest
  <> "  libtemporal_nif-x86_64-unknown-linux-gnu.so\n"
}

fn install_into(path: String, checksums: String, fetch) {
  install.install(
    checksums:,
    version: "1.2.3",
    target: "x86_64-unknown-linux-gnu",
    into: path,
    fetch:,
  )
}

pub fn a_download_with_the_recorded_digest_is_installed_test() {
  let path = scratch()
  let key = "install-url-" <> support.unique()
  let assert Ok(message) =
    install_into(path, checksums_for(abc), serving(<<"abc":utf8>>, key))
  assert string.contains(message, "v1.2.3")
  assert string.contains(message, abc)
  assert support.read_file(path) == Ok(<<"abc":utf8>>)
  let assert Error(_) = support.read_file(path <> ".part")
  // It asked for this version's library for this target, and nothing else.
  assert support.recorded(key)
    == "https://github.com/dangdennis/gleam-temporal-sdk/releases/download/v1.2.3/libtemporal_nif-x86_64-unknown-linux-gnu.so"
}

pub fn a_download_with_another_digest_is_deleted_test() {
  let path = scratch()
  // A library already in place, from an earlier install, is left as it is.
  support.write_file(path, <<"the old library":utf8>>)
  let assert Error(message) =
    install_into(path, checksums_for(abc), serving(<<"abd":utf8>>, "unused"))
  assert string.contains(message, "expected SHA-256 " <> abc)
  assert string.contains(
    message,
    "downloaded      a52d159f262b2c6ddb724a61840befc36eb30c88877a4030b65cbe86298449c9",
  )
  assert support.read_file(path) == Ok(<<"the old library":utf8>>)
  let assert Error(_) = support.read_file(path <> ".part")

  // With nothing in place before, nothing is in place after.
  let path = scratch()
  let assert Error(_) =
    install_into(path, checksums_for(abc), serving(<<>>, "unused"))
  let assert Error(_) = support.read_file(path)
  let assert Error(_) = support.read_file(path <> ".part")
  Nil
}

pub fn nothing_is_downloaded_without_a_checksum_to_verify_it_test() {
  let path = scratch()
  let never = fn(_, _) { panic as "downloaded without a checksum" }
  let assert Error(_) =
    install_into(path, "# Filled in by the Release workflow.\n", never)
  let assert Error(_) =
    install.install(
      checksums: checksums_for(abc),
      version: "1.2.4",
      target: "x86_64-unknown-linux-gnu",
      into: path,
      fetch: never,
    )
  let assert Error(_) =
    install.install(
      checksums: checksums_for(abc),
      version: "1.2.3",
      target: "aarch64-apple-darwin",
      into: path,
      fetch: never,
    )
  let assert Error(_) = support.read_file(path)
  Nil
}

pub fn a_failed_download_leaves_nothing_behind_test() {
  let path = scratch()
  let failing = fn(_, partial) {
    support.write_file(partial, <<"half of i":utf8>>)
    Error("curl: (22) The requested URL returned error: 404\n")
  }
  let assert Error(message) = install_into(path, checksums_for(abc), failing)
  assert string.contains(message, "could not download https://github.com/")
  assert string.contains(
    message,
    "error: 404\nCompile the library with bin/build-nif",
  )
  let assert Error(_) = support.read_file(path)
  let assert Error(_) = support.read_file(path <> ".part")
  Nil
}
