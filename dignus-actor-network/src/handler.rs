use crate::session::Session;
use std::sync::Arc;

pub trait HostHandler: Send + 'static {
    fn on_accepted(&mut self, _session: Arc<Session>) {}
    fn on_data(&mut self, session: &Arc<Session>, data: &[u8]);
    fn on_disconnected(&mut self, _session_id: u64) {}
}
