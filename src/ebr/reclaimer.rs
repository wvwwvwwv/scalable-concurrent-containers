use super::Reader;

/// [`Reclaimer`] is a container where unowned instances stay until they become completely unreachable.
pub struct Reclaimer {}

impl Reclaimer {
    pub fn reader() -> Reader {
        Reader {}
    }
}
