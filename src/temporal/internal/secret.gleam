//// A value that must not be printed: an API key, a private key, an encryption key.
////
//// Opacity in Gleam is checked by the compiler only; at run time `string.inspect`, a crash
//// report or a log line prints whatever a record holds. A function is the one value whose
//// contents are never printed, it shows as `//fn() { ... }`, so the secret is held inside
//// one.

/// A secret value. Printing it shows a function, never the value.
pub opaque type Secret(a) {
  Secret(reveal: fn() -> a)
}

/// Wrap a value so that it cannot be printed.
pub fn new(value: a) -> Secret(a) {
  Secret(fn() { value })
}

/// The value, for the code that must use it. Do not log or inspect the result.
pub fn reveal(secret: Secret(a)) -> a {
  secret.reveal()
}
