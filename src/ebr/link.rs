pub(super) trait Link {
    fn set(&mut self, next_ptr: *const dyn Link);
    fn dealloc(&mut self) -> *mut dyn Link;
}