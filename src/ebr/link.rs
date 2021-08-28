pub(super) trait Link {
    fn next(&self) -> *const dyn Link;
    fn set(&mut self, next_ptr: *const dyn Link);
    fn dealloc(&mut self);
}