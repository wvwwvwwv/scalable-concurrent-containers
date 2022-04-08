//! [`async_yield`].

use crate::ebr::Barrier;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Yields the current executor.
///
/// The code is copied from
/// [`tokio`](https://docs.rs/tokio/1.13.0/src/tokio/task/yield_now.rs.html#38-59).
pub(crate) async fn async_yield() {
    #[derive(Default)]
    struct YieldOnce(bool);
    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
    YieldOnce::default().await;
}

/// [`AwaitableBarrier`] enables asynchronous code to use a [`Barrier`].
///
/// The user has to manually drop the [`Barrier`] before each `await` by calling
/// [`AwaitableBarrier::drop_barrier_and_yield`].
#[derive(Default)]
pub(super) struct AwaitableBarrier {
    barrier: Option<Barrier>,
}

impl AwaitableBarrier {
    /// Returns a reference to the [`Barrier`].
    pub(super) fn barrier(&mut self) -> &Barrier {
        self.barrier.get_or_insert_with(Barrier::new)
    }

    /// Yields the current executor after dropping the barrier.
    pub(super) async fn drop_barrier_and_yield(&mut self) {
        self.barrier.take();
        async_yield().await;
    }
}

#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for AwaitableBarrier {
    // It is actually unsafe if the user forgets to drop the [`Barrier`] before an `await`.
}
