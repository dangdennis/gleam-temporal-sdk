//! How values cross between Gleam and Rust. `Wire` reads and writes the Erlang term a Gleam
//! value compiles to; `bridge.rs`, which is generated, implements it for every type in
//! `temporal/internal/bridge`, and this file covers the types those are built from.
//!
//! A Gleam record `Foo(a, b)` is the tuple `{foo, A, B}`, a constructor without fields is a
//! bare atom, and `Option` is `{some, X}` or `none`.

use crate::convert::{decode_failure, decode_payload, encode_failure, encode_payload};
use rustler::{
    Atom, Binary, Encoder, Env, MapIterator, NewBinary, Term,
    types::tuple::{get_tuple, make_tuple},
};
use std::collections::HashMap;
use temporalio_common::protos::temporal::api::{common::v1::Payload, failure::v1::Failure};

pub type DecodeResult<T> = Result<T, String>;

pub trait Wire: Sized {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a>;
    fn from_term(term: Term) -> DecodeResult<Self>;
}

/// A Gleam `BitArray`. A plain `Vec<u8>` would read as a list of integers.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Bytes(pub Vec<u8>);

pub fn atom<'a>(env: Env<'a>, name: &str) -> Term<'a> {
    Atom::from_str(env, name)
        .expect("atom names are short ASCII")
        .to_term(env)
}

/// A Gleam record: `{tag, Fields...}`.
pub fn record<'a>(env: Env<'a>, tag: &str, fields: &[Term<'a>]) -> Term<'a> {
    let mut terms = Vec::with_capacity(fields.len() + 1);
    terms.push(atom(env, tag));
    terms.extend_from_slice(fields);
    make_tuple(env, &terms)
}

/// Split a record into its tag and fields; a bare atom is a record without fields.
pub fn untag(term: Term) -> DecodeResult<(String, Vec<Term>)> {
    if term.is_atom() {
        return Ok((term.atom_to_string().map_err(|_| "bad atom")?, vec![]));
    }
    let parts = get_tuple(term).map_err(|_| "expected a record".to_string())?;
    let tag = parts
        .first()
        .and_then(|t| t.atom_to_string().ok())
        .ok_or("record without a tag")?;
    Ok((tag, parts[1..].to_vec()))
}

/// Read field `index` of a record, naming the field if it has the wrong shape.
pub fn at<T: Wire>(fields: &[Term], index: usize, name: &str) -> DecodeResult<T> {
    T::from_term(fields[index]).map_err(|reason| format!("{name}: {reason}"))
}

/// A `BitArray` from borrowed bytes.
pub fn binary<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut binary = NewBinary::new(env, bytes.len());
    binary.as_mut_slice().copy_from_slice(bytes);
    binary.into()
}

pub fn unexpected(type_name: &str, tag: &str, count: usize) -> String {
    format!("{type_name}: no constructor `{tag}` with {count} fields; run bin/codegen")
}

fn decoded<'a, T: rustler::Decoder<'a>>(term: Term<'a>, expected: &str) -> DecodeResult<T> {
    term.decode().map_err(|_| format!("expected {expected}"))
}

impl Wire for String {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.encode(env)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        decoded(term, "a String")
    }
}

impl Wire for i64 {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.encode(env)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        decoded(term, "an Int")
    }
}

impl Wire for u64 {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.encode(env)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        decoded(term, "a non-negative Int")
    }
}

impl Wire for f64 {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.encode(env)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        decoded(term, "a Float")
    }
}

impl Wire for bool {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.encode(env)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        decoded(term, "a Bool")
    }
}

impl Wire for () {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        atom(env, "nil")
    }
    fn from_term(_: Term) -> DecodeResult<Self> {
        Ok(())
    }
}

impl Wire for Bytes {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        binary(env, &self.0)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        let binary: Binary = decoded(term, "a BitArray")?;
        Ok(Bytes(binary.as_slice().to_vec()))
    }
}

impl<T: Wire> Wire for Vec<T> {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.iter()
            .map(|item| item.to_term(env))
            .collect::<Vec<_>>()
            .encode(env)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        let items: Vec<Term> = decoded(term, "a List")?;
        items.into_iter().map(T::from_term).collect()
    }
}

impl<T: Wire> Wire for Option<T> {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            Some(value) => record(env, "some", &[value.to_term(env)]),
            None => atom(env, "none"),
        }
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        let (tag, fields) = untag(term)?;
        match (tag.as_str(), fields.as_slice()) {
            ("some", [value]) => Ok(Some(T::from_term(*value)?)),
            ("none", []) => Ok(None),
            _ => Err("expected an Option".to_string()),
        }
    }
}

/// A type that holds itself, as `Failure` does its cause, is boxed in Rust and not in Gleam.
impl<T: Wire> Wire for Box<T> {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.as_ref().to_term(env)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        T::from_term(term).map(Box::new)
    }
}

impl<T: Wire, E: Wire> Wire for Result<T, E> {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            Ok(value) => record(env, "ok", &[value.to_term(env)]),
            Err(error) => record(env, "error", &[error.to_term(env)]),
        }
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        let (tag, fields) = untag(term)?;
        match (tag.as_str(), fields.as_slice()) {
            ("ok", [value]) => Ok(Ok(T::from_term(*value)?)),
            ("error", [error]) => Ok(Err(E::from_term(*error)?)),
            _ => Err("expected a Result".to_string()),
        }
    }
}

impl<T: Wire> Wire for HashMap<String, T> {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        let mut map = Term::map_new(env);
        for (key, value) in self {
            map = map
                .map_put(key.as_str(), value.to_term(env))
                .expect("map_put on a map");
        }
        map
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        let entries = MapIterator::new(term).ok_or("expected a Dict")?;
        entries
            .map(|(key, value)| Ok((decoded(key, "a String key")?, T::from_term(value)?)))
            .collect()
    }
}

macro_rules! tuple {
    ($count:expr; $($item:ident $index:tt),+) => {
        impl<$($item: Wire),+> Wire for ($($item,)+) {
            fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
                make_tuple(env, &[$(self.$index.to_term(env)),+])
            }
            fn from_term(term: Term) -> DecodeResult<Self> {
                let parts = get_tuple(term).map_err(|_| "expected a tuple".to_string())?;
                if parts.len() != $count {
                    return Err(format!("expected a tuple of {}", $count));
                }
                Ok(($($item::from_term(parts[$index])?,)+))
            }
        }
    };
}
tuple!(2; A 0, B 1);
tuple!(3; A 0, B 1, C 2);
tuple!(4; A 0, B 1, C 2, D 3);

// `temporal/codec.Payload` and `temporal/failure.Failure` are read straight into core's
// messages, by hand, in convert.rs.

impl Wire for Payload {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        encode_payload(env, self)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        decode_payload(term)
    }
}

impl Wire for Failure {
    fn to_term<'a>(&self, env: Env<'a>) -> Term<'a> {
        encode_failure(env, self)
    }
    fn from_term(term: Term) -> DecodeResult<Self> {
        decode_failure(term)
    }
}
