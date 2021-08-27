/// [`Ptr`] points to an instance.
pub struct Ptr<'r, T> {
    _phantom: std::marker::PhantomData<&'r T>,
}
