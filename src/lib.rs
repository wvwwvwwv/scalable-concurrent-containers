#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Scalable concurrent containers.
//!
//! # [`EBR`](ebr)
//!
//! The [`ebr`] module implements epoch-based reclamation for container types in this crate.
//!
//! # [Awaitable concurrent containers](awaitable)
//!
//! Awaitable concurrent containers that can be used in an asynchronous task are implemented. An
//! awaitable container simply yields execution back to the asynchronous runtime when a conflict is
//! detected without causing a CPU stall or holding an executor thread.
//!
//! * [`HashMap`](awaitable::HashMap).
//!
//! # [Concurrent containers](concurrent)
//!
//! Highly efficient and scalable concurrent containers.
//!
//! * [`LinkedList`](concurrent::LinkedList).
//! * [`HashMap`](concurrent::HashMap).
//! * [`HashIndex`](concurrent::HashIndex).
//! * [`TreeIndex`](concurrent::TreeIndex).

pub mod awaitable;
pub mod concurrent;
pub mod ebr;

mod tests;
