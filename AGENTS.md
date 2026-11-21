# Working in this repository

`temporal_gleam` is a Temporal SDK for Gleam on the BEAM. A Rust NIF exposes Temporal's
`sdk-core` to Erlang, and a Gleam runtime on top of it runs workflow and activity code.

To write code that uses the SDK, read [skills/temporal-gleam/SKILL.md](skills/temporal-gleam/SKILL.md)
and copy from [examples/](examples/README.md). This file is about changing the SDK itself.

## Layout

| Path | Holds |
|---|---|
| `src/temporal/*.gleam`, `src/temporal/workflow/` | The public API, one module per concern |
| `src/temporal/internal/` | The workflow runtime, pollers and the NIF boundary (`bridge.gleam`). Not public |
| `src/*.erl` | Erlang FFI: the NIF loader, process and logger helpers, the installer |
| `native/temporal_nif/` | The Rust NIF. `src/bridge.rs` is generated |
| `codegen/` | A Gleam project that generates `bridge.rs` from `bridge.gleam` |
| `test/` | The SDK's tests; `test/conformance/` is the cross-SDK harness with its TypeScript twins |
| `examples/` | A separate Gleam project: one runnable module per pattern, and a test for each |
| `skills/temporal-gleam/` | The skill file for agents that use the SDK |

## Commands

Needs Gleam 1.19+, Erlang/OTP 27+, Rust 1.88+, the Temporal CLI, and Node.js for
conformance. The scripts start a development server if none is running; do not run two at
once, because they share localhost:7233.

| Command | Does | Run it |
|---|---|---|
| `bin/build-nif` | Compiles the NIF into `priv/` | After any change under `native/`; the other scripts call it |
| `bin/test` | Every SDK test against a dev server | Before finishing any change to `src/`, `native/` or `test/` |
| `bin/conformance` | `bin/test`, then replays this SDK's histories on the TypeScript SDK and prints a table | After any change to the workflow runtime or to what commands the SDK emits |
| `bin/examples` | Runs every example and checks its result | After any change to the public API, `examples/` or the skill |
| `bin/load` | Thousands of workflows while workers are killed | After changes to workers, pollers or process handling |
| `bin/codegen` | Regenerates `native/temporal_nif/src/bridge.rs` | After changing a type in `bridge.gleam` |
| `bin/build-nif-musl` | Compiles the NIF for musl in an Alpine container and loads it in an Alpine Erlang. Needs Docker | After changing how the NIF is built or loaded; the release workflow runs it |

CI also checks `gleam format` (here and in `examples/`), `cargo fmt` and `cargo clippy`.

## Rules that are easy to break

- **The boundary is generated.** `src/temporal/internal/bridge.gleam` is the source for
  `native/temporal_nif/src/bridge.rs`. Change the Gleam type, run `bin/codegen`, fix what
  the Rust compiler points at in `convert.rs`, and commit the result. Never edit `bridge.rs`.
- **Runtime changes can change emitted commands.** Anything under
  `src/temporal/internal/runtime*` or in how `workflow.gleam` and `workflow/*.gleam` push
  commands can alter their order or numbering, which breaks replay of existing histories
  and of other SDKs' histories. The `bin/conformance` table must stay all `replays` /
  `rejected`. A new workflow feature needs a twin in `test/conformance_test.gleam` and in
  `test/conformance/typescript/workflows.js`.
- **Public API conventions.** Options are a constructor that takes what is required plus
  one `with_*` setter per optional setting, used in a pipeline; never ask callers to
  spread a record. Retry policies come from `temporal/retry` everywhere. Signals, queries,
  updates, child workflows, local activities and Nexus calls each have a module under
  `temporal/workflow/` with the same verbs (`start`, `execute`, `next`, `receive`). JSON
  shapes are described with `temporal/schema`, or `codec.json` for a hand-written pair.
  Logging takes `temporal/log` fields. Helpers that must be `pub` carry `@internal`.
- **Comments are concise and written for a person.** Every public item has a `///` doc
  comment, every module a `////` one. Say what a thing is for and what is not obvious; do
  not narrate the code.
- **A public API change is not done until the examples and the skill match it.** Update
  the affected module under `examples/src/examples/`, its test, `examples/README.md`,
  `skills/temporal-gleam/SKILL.md`, `README.md` and `docs/guide.md`. The skill's snippets
  are copied verbatim from the examples and it quotes error messages from the source:
  copy again when either changes, do not retype.
- **Secrets are held in functions.** `string.inspect`, crash reports and log lines print
  whatever a record holds, opaque or not, but never the inside of a function. An API key,
  a private key or an encryption key is stored as a `temporal/internal/secret.Secret` and
  revealed only where it is used; setters still take the plain value. A bridge type that
  carries one to Rust is listed in `secret` in `codegen/src/codegen.gleam`, which gives it
  a `Debug` that prints `<redacted>`.
- **Failure text leaves the machine unencrypted.** A payload codec does not cover a
  failure's message or stack trace, so never build one from a value the user's code was
  handling: no `string.inspect` of an error or an exit reason in a message. A crash is
  described by `temporal_ffi:crash/3` (kind, location, arities) and logged in full
  locally with `bridge.log`. What a Nexus handler answers goes to another team: generic
  text to the caller, the detail to the log. `client.with_encoded_failure_attributes`
  must keep the TypeScript SDK's `encodeCommonAttributes` format, and headers must stay
  outside the codec while TypeScript leaves them there; `test/failure_privacy_test.gleam`
  checks both against the TypeScript SDK.
- **Log fields are never atoms.** `bridge.log` takes the SDK's own identity keys, which
  `temporal_ffi:identity_key/1` maps to atoms from a fixed table, apart from the caller's
  fields, which stay binaries in one map under `fields`. Do not `binary_to_atom` a string
  that did not come from this repository's source.
- **Releasing.** Set the version in `gleam.toml`, run `gleam deps update temporal_gleam`
  in `examples/` so its lock file follows, and update the `ref = "v..."` lines in the
  docs. Once CI is green on main, run the Prepare release workflow: it builds the native
  library for every target, holds the files in a draft release, and opens a pull request
  that writes their SHA-256 sums to `priv/checksums.txt`. Merge it, wait for CI, and push
  the tag `v<version>` on the merge commit: the Release workflow checks that the tagged
  commit is the built one plus its checksums and publishes the draft.
- **The installer installs only what the tagged commit vouches for.**
  `gleam run -m temporal/install` reads `priv/checksums.txt` from the package it runs in
  and refuses a download whose SHA-256 is not there, and refuses to download at all when
  the file is not for the package's own version. The file is a first line
  `# v<version> <commit the libraries were built from>` and then `sha256sum` lines,
  `<64 hex>  libtemporal_nif-<rust target>.so`; between releases it is a single comment,
  which is why the installer does nothing in a checkout (`bin/build-nif` is how the
  library gets there). Never write the file by hand, never add a way to skip the check
  or to install another version's library, and keep the reader in `install.gleam` and
  the writer in the workflow in step. The targets are `x86_64` and `aarch64` for each of
  `unknown-linux-gnu`, `unknown-linux-musl` and `apple-darwin`; a new one needs an entry
  in the workflow's matrix and a case in `install.target`, with its architecture strings
  in `test/install_test.gleam`.
