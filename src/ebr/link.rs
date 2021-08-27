pub(super) trait Link {
    fn next(&self) -> Option<&dyn Link>;
    fn set(&mut self, next: &dyn Link);
}