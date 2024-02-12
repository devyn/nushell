use std::sync::{atomic::{AtomicBool, Ordering::Relaxed}, Arc, Weak};

use crate::protocol::StreamId;

/// Stores interrupt flags for each stream currently being written
#[derive(Debug)]
pub(crate) struct StreamInterruptFlags {
    flags: Vec<(StreamId, Weak<AtomicBool>)>,
}

impl StreamInterruptFlags {
    pub fn new() -> StreamInterruptFlags {
        StreamInterruptFlags { flags: vec![] }
    }

    /// Register a new interrupt flag and get the lifecycle handle
    pub fn register(&mut self, id: StreamId) -> InterruptHandle {
        // Check if we already have the flag
        if let Some(index) = self.flags.iter().position(|(flag_id, _)| *flag_id == id) {
            // Try to upgrade - this could fail
            if let Some(flag) = self.flags[index].1.upgrade() {
                // Found and living, so just return that instead
                return InterruptHandle { flag };
            }
        }

        // Clean up before adding the new flag
        self.cleanup();

        let flag = Arc::new(AtomicBool::new(false));
        let weak_flag = Arc::downgrade(&flag);

        self.flags.push((id, weak_flag));

        InterruptHandle { flag }
    }

    /// Interrupt a stream
    pub fn interrupt(&mut self, id: StreamId) {
        for (flag_id, flag) in self.flags.iter_mut() {
            if *flag_id == id {
                // Only if the flag is still alive
                if let Some(flag_strong) = flag.upgrade() {
                    flag_strong.store(true, Relaxed);
                }
            }
        }
    }

    /// Remove dead flags
    fn cleanup(&mut self) {
        self.flags.retain(|(_, flag)| flag.strong_count() > 0)
    }
}

/// A handle for telling when a stream has been interrupted.
#[derive(Debug, Clone)]
pub(crate) struct InterruptHandle {
    flag: Arc<AtomicBool>,
}

impl InterruptHandle {
    /// Returns true if the stream has been interrupted.
    pub fn is_interrupted(&self) -> bool {
        self.flag.load(Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_interrupt() {
        let mut flags = StreamInterruptFlags::new();
        let handle = flags.register(0);
        assert!(!handle.is_interrupted());
        flags.interrupt(0);
        assert!(handle.is_interrupted());
    }

    #[test]
    fn register_and_interrupt_twice() {
        let mut flags = StreamInterruptFlags::new();
        let handle = flags.register(0);
        assert!(!handle.is_interrupted());
        flags.interrupt(0);
        assert!(handle.is_interrupted());
        flags.interrupt(0);
        assert!(handle.is_interrupted());
    }

    #[test]
    fn interrupt_unregistered() {
        let mut flags = StreamInterruptFlags::new();
        flags.interrupt(1);
    }

    #[test]
    fn register_same_twice_then_interrupt_should_share_state() {
        let mut flags = StreamInterruptFlags::new();
        let handle0 = flags.register(0);
        assert!(!handle0.is_interrupted());
        let handle1 = flags.register(0);
        assert!(!handle1.is_interrupted());
        flags.interrupt(0);
        assert!(handle0.is_interrupted());
        assert!(handle1.is_interrupted());
    }

    #[test]
    fn different_handles_should_not_share_state() {
        let mut flags = StreamInterruptFlags::new();
        let handle0 = flags.register(0);
        assert!(!handle0.is_interrupted());
        let handle1 = flags.register(1);
        assert!(!handle1.is_interrupted());
        flags.interrupt(0);
        assert!(handle0.is_interrupted());
        assert!(!handle1.is_interrupted());
        flags.interrupt(1);
        assert!(handle1.is_interrupted());
    }

    #[test]
    fn handle_cleanup() {
        let mut flags = StreamInterruptFlags::new();
        let handle0 = flags.register(0);
        assert_eq!(1, flags.flags.len());
        flags.cleanup();
        assert_eq!(1, flags.flags.len());
        drop(handle0);
        flags.cleanup();
        assert_eq!(0, flags.flags.len());
    }

    #[test]
    fn cleanup_during_register() {
        let mut flags = StreamInterruptFlags::new();
        let handle0 = flags.register(0);
        assert_eq!(1, flags.flags.len());
        drop(handle0);
        let handle1 = flags.register(1);
        assert_eq!(1, flags.flags.len());
        drop(handle1);
    }
}
