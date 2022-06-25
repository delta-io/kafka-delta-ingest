use std::time::{Duration, Instant};

use crate::errors::TimerError;

/// An ergonomic timer that can be checked for expiration and reset.
#[derive(Debug)]
pub struct Timer {
    start: Option<Instant>,
    duration: Duration,
}

impl Timer {
    /// Constructs a timer that expires after the given duration has elapsed.
    /// This timer must be reset once to initialize the start time before checking expiration.
    pub fn new(duration: Duration) -> Self {
        Self {
            start: None,
            duration,
        }
    }

    /// Constructs a timer with a start time of "now" that expires after the given duration has elapsed.
    pub fn new_from_now(duration: Duration) -> Self {
        Self {
            start: Some(Instant::now()),
            duration,
        }
    }

    /// Returns true if the timer is expired based on the start time and duration.
    pub fn is_expired(&self) -> Result<bool, TimerError> {
        self.start
            .map(|instant| instant.elapsed() >= self.duration)
            .ok_or(TimerError::Uninitialized)
    }

    /// Resets the start time for the timer.
    pub fn reset(&mut self) {
        self.start = Some(Instant::now())
    }
}
