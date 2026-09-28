//! The Felix client conformance kit.
//!
//! Apache-2.0 on purpose: a third party writing a Felix client in a language
//! nobody here has thought of should be able to vendor the catalogue and the
//! verifier without taking a copyleft dependency. What they cannot vendor is a
//! broker to test against — that is AGPL, and it is why the *fixture server*
//! lives in `felix-cluster` while the *specification* and the *verdict* live
//! here.
//!
//! See [`kit`] for the contract, and `felix-cluster client-fixture` for
//! something to run it against. [`link`] is the interposer the
//! connection-fault scenarios run through.
pub mod kit;
pub mod link;
