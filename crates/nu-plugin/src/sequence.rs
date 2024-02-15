use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use nu_protocol::ShellError;

/// Implements an atomically incrementing sequential series of numbers
#[derive(Debug, Default)]
pub(crate) struct Sequence(AtomicUsize);

impl Sequence {
    /// Return the next available id from a sequence, returning an error on overflow
    #[track_caller]
    pub(crate) fn next(&self) -> Result<usize, ShellError> {
        // It's totally safe to use Relaxed ordering here, as there aren't other memory operations
        // that depend on this value having been set for safety
        //
        // We're only not using `fetch_add` so that we can check for overflow, as wrapping with the
        // identifier would lead to a serious bug - however unlikely that is.
        self.0
            .fetch_update(Relaxed, Relaxed, |current| current.checked_add(1))
            .map_err(|_| ShellError::NushellFailedHelp {
                msg: "an accumulator for identifiers overflowed".into(),
                help: format!("see {}", std::panic::Location::caller()),
            })
    }
}
