//! Test-only serialisation of the process environment.
//!
//! Rust 2024 made `std::env::set_var`/`remove_var` `unsafe` because the
//! environment is process-global while the test harness runs tests on
//! parallel threads. Every env mutation in this crate's tests goes through
//! [`lock`] and [`set`] — one lock for the whole test binary, held for as
//! long as the test reads env vars — so the safety argument lives in exactly
//! one place. Both test modules (`host`, `handlers`) share this lock: a
//! per-module mutex would leave two threads free to mutate the environment
//! at once.

use std::sync::{Mutex, MutexGuard};

static ENV: Mutex<()> = Mutex::new(());

/// Lock the process environment for this test.
///
/// Hold the returned guard for the whole test, including any reads that
/// would otherwise race a sibling's mutation.
pub(crate) fn lock() -> MutexGuard<'static, ()> {
    // A poisoned lock only means some other test panicked while holding it;
    // the environment is not invariant-critical for these tests.
    ENV.lock().unwrap_or_else(|e| e.into_inner())
}

/// Set `key` to the path `value` while the environment is locked.
///
/// The caller must hold the [`lock`] guard for as long as it reads env vars.
pub(crate) fn set(key: &str, value: &std::path::Path) {
    // SAFETY: the caller holds the shared `ENV` lock, which serialises every
    // env mutation in this test binary.
    unsafe { std::env::set_var(key, value) };
}

/// Remove `key` while the environment is locked.
///
/// The caller must hold the [`lock`] guard for as long as it reads env vars.
pub(crate) fn remove(key: &str) {
    // SAFETY: the caller holds the shared `ENV` lock, which serialises every
    // env mutation in this test binary.
    unsafe { std::env::remove_var(key) };
}
