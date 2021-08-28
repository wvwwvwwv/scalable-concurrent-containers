use super::Reclaimer;

/// [`Reader`] allows the user to read [`AtomicPtr`].
pub struct Reader {
    reclaimer_ptr: *mut Reclaimer,
}

impl Reader {
    pub(super) fn new(reclaimer_ptr: *mut Reclaimer) -> Reader {
        Reader { reclaimer_ptr }
    }
}
