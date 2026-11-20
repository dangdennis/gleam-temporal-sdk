//// Which prebuilt library `temporal/install` picks for a machine, from what the machine
//// reports. The architecture strings are the ones real VMs print.

import gleam/string
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
