#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Scalable concurrent containers.
//!
//! # [`EBR`](ebr)
//!
//! The [`ebr`] module implements epoch-based reclamation for container types in this crate.
//!
//! # [Synchronous concurrent containers](sync)
//!
//! Synchronous concurrent containers, such as [`HashMap`](sync::HashMap),
//! [`HashIndex`](sync::HashIndex), and [`TreeIndex`](sync::TreeIndex) are implemented.
//!
//! # [Asynchronous concurrent containers](async)
//!
//! Asynchronous concurrent containers are implemented.

pub mod awaitable;
pub mod ebr;
pub mod sync;

mod tests;
