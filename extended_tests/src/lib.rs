#![deny(warnings, clippy::all, clippy::pedantic)]

#[cfg(test)]
mod exp;

#[cfg(test)]
mod oom;

#[cfg(test)]
pub(crate) static SERIALIZER: std::sync::Mutex<()> = std::sync::Mutex::new(());
