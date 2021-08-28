use super::arc::Underlying;

/// [`Ptr`] points to an instance.
pub struct Ptr<'r, T> {
    ptr: *const Underlying<T>,
    _phantom: std::marker::PhantomData<&'r T>,
}
