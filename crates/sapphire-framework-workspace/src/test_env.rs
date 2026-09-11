//! Test-only serialisation helper for process-global env vars.
//!
//! Rust 2024 made `std::env::set_var`/`remove_var` `unsafe` because the
//! environment is process-global while the test harness runs tests in
//! parallel.  Every env mutation in this crate's tests goes through
//! [`TestEnv`] (holding the shared lock for as long as the test reads env
//! vars as well), so the safety argument lives in exactly one place.

use std::sync::Mutex;

static ENV: Mutex<()> = Mutex::new(());

/// Serialises env mutation (and the env reads that race with it) across the
/// crate's test modules.  Hold the returned guard for the whole test.
pub(crate) struct TestEnv;

impl TestEnv {
    /// Lock the process environment for this test.
    #[must_use = "the guard must be held while the test reads env vars"]
    pub(crate) fn lock() -> std::sync::MutexGuard<'static, ()> {
        // A poisoned lock only means some other test panicked while holding
        // it; env state is not invariant-critical for these tests.
        ENV.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Set an env var while the environment is locked (see the module docs).
    ///
    /// # Safety
    /// The caller must hold the [`TestEnv::lock`] guard for as long as it
    /// reads env vars.
    pub(crate) fn set(key: &str, value: &std::path::Path) {
        // SAFETY: the caller holds the shared `ENV` lock, which serialises
        // every env mutation in this crate's tests.
        unsafe { std::env::set_var(key, value) };
    }

    /// Remove an env var while the environment is locked (see the module docs).
    ///
    /// # Safety
    /// The caller must hold the [`TestEnv::lock`] guard for as long as it
    /// reads env vars.
    pub(crate) fn remove(key: &str) {
        // SAFETY: the caller holds the shared `ENV` lock, which serialises
        // every env mutation in this crate's tests.
        unsafe { std::env::remove_var(key) };
    }
}
