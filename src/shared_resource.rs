use std::sync::{atomic::AtomicBool, atomic::AtomicU32, atomic::Ordering, Arc};
use tokio::sync::{Mutex, Notify, OwnedMutexGuard};
struct RequestCount {
    count: AtomicU32,
    notify: Notify,
}

pub struct Request<T> {
    resource: Arc<Mutex<T>>,
    req_count: Arc<RequestCount>,
    requesting: AtomicBool,
}

impl<T> Clone for Request<T> {
    fn clone(&self) -> Self {
        Self {
            resource: self.resource.clone(),
            req_count: self.req_count.clone(),
            requesting: AtomicBool::new(false),
        }
    }
}

impl<T> Request<T> {
    pub fn new(value: T) -> Request<T> {
        Request {
            resource: Arc::new(Mutex::new(value)),
            req_count: Arc::new(RequestCount {
                count: AtomicU32::new(0),
                notify: Notify::new(),
            }),
            requesting: AtomicBool::new(false),
        }
    }

    /// Request access to a resource and wait for it to become available
    pub async fn request(&self) -> OwnedMutexGuard<T> {
        self.req_count.count.fetch_add(1, Ordering::AcqRel);
        self.requesting.store(true, Ordering::Release);
        self.req_count.notify.notify_waiters();
        let guard = self.resource.clone().lock_owned().await;
        self.req_count.count.fetch_sub(1, Ordering::AcqRel);
        self.requesting.store(false, Ordering::Release);
        guard
    }

    /// Wait until some other task is requesting access to the resource
    pub async fn requested(&self) {
        loop {
            let changed = self.req_count.notify.notified();
            let count = self.req_count.count.load(Ordering::Acquire);
            if count > 0 {
                break;
            }
            changed.await;
        }
    }
}
impl<T> Drop for Request<T> {
    fn drop(&mut self) {
        if self.requesting.load(Ordering::Acquire) {
            self.req_count.count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Request, ResourceGuard};
    #[tokio::test]
    async fn request_test() {
        let req = Request::new(8);
        let v = req.request().await;
        assert_eq!(*v, 8);
        let task_req = req.clone();
        tokio::spawn(async move { task_req.request().await });
        ResourceGuard::requested(&v).await;
    }
}
