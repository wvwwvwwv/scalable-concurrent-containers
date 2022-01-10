//! [`async_yield`].

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// From [`tokio`](https://docs.rs/tokio/1.13.0/src/tokio/task/yield_now.rs.html#38-59).
pub async fn async_yield() {
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
