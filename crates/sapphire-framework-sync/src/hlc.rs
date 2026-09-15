//! Hybrid logical clock.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// How far ahead of the local wall clock a remote timestamp may pull the local clock.
pub const MAX_DRIFT_MS: u64 = 24 * 60 * 60 * 1000;

/// Hybrid logical clock value, ordered by wall time and then the logical counter.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Hlc {
    pub wall_ms: u64,
    pub logical: u32,
}

/// Source of wall-clock time, injectable for tests.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// The system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

impl Hlc {
    /// The timestamp of a new local event; strictly greater than `self`.
    pub fn tick(self, now_ms: u64) -> Hlc {
        if now_ms > self.wall_ms {
            Hlc {
                wall_ms: now_ms,
                logical: 0,
            }
        } else if self.logical == u32::MAX {
            Hlc {
                wall_ms: self.wall_ms + 1,
                logical: 0,
            }
        } else {
            Hlc {
                wall_ms: self.wall_ms,
                logical: self.logical + 1,
            }
        }
    }

    /// Fold a received timestamp into the local clock. A remote wall time beyond
    /// `now + MAX_DRIFT_MS` is logged and clamped, so one skewed device cannot drag
    /// every clock forward.
    pub fn observe(self, remote: Hlc, now_ms: u64) -> Hlc {
        let cap = now_ms.saturating_add(MAX_DRIFT_MS);
        let remote = if remote.wall_ms > cap {
            tracing::warn!(
                remote_wall_ms = remote.wall_ms,
                now_ms,
                "remote clock is more than 24h ahead; clamping"
            );
            Hlc {
                wall_ms: cap,
                logical: 0,
            }
        } else {
            remote
        };
        self.max(remote)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_strictly_monotonic_even_when_the_wall_clock_stalls() {
        let a = Hlc::default().tick(100);
        let b = a.tick(100);
        let c = b.tick(50);
        assert!(a < b && b < c);
        assert_eq!(
            a,
            Hlc {
                wall_ms: 100,
                logical: 0
            }
        );
        assert_eq!(
            c,
            Hlc {
                wall_ms: 100,
                logical: 2
            }
        );
    }

    #[test]
    fn observe_takes_the_later_clock() {
        let local = Hlc {
            wall_ms: 10,
            logical: 3,
        };
        let remote = Hlc {
            wall_ms: 20,
            logical: 1,
        };
        assert_eq!(local.observe(remote, 15), remote);
        assert!(local.observe(remote, 15).tick(15) > remote);
    }

    #[test]
    fn observe_clamps_far_future_remotes() {
        let remote = Hlc {
            wall_ms: 1_000 + MAX_DRIFT_MS + 5,
            logical: 0,
        };
        let got = Hlc::default().observe(remote, 1_000);
        assert_eq!(
            got,
            Hlc {
                wall_ms: 1_000 + MAX_DRIFT_MS,
                logical: 0
            }
        );
    }
}
