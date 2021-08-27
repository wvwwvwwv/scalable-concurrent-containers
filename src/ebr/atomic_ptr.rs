/// [`AtomicPtr`] owns the underlying instance.
pub struct AtomicPtr<T> {
    _phantom: std::marker::PhantomData<T>,
}
