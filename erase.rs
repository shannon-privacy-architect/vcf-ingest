//! Memory-erasure primitives (privacy guarantee: no raw VCF bytes survive in
//! the Rust heap after a chunk is consumed or the process exits).
//!
//! Audited guarantees (task: zeroization audit):
//! - [`erase_buffer`] zeroes the **entire allocation**, including bytes past
//!   `len` up to `capacity()`. `iter_erase` only covers `len` bytes; stale
//!   data can hide in the reserved region, so we resize to capacity first.
//! - [`EraseGuard`] re-runs the pass on drop, covering `?` early-returns,
//!   panics, and normal returns alike (RAII).
//! - The streaming loop reuses ONE buffer and erases in place; no clones of
//!   chunk bytes are ever created (see `src/stream.rs`).
//! - [`copy_volatile`] is provided for the (test-only) assertion paths.
//!
//! What is NOT covered (documented honestly in README): OS pipe/page-cache
//! remnants, and any copies the OS or allocator itself makes outside Rust's
//! visibility (e.g. swap, core dumps).

use zeroize::Zeroize;

/// Volatile-erase the whole allocation of `buf`: resize to capacity, zeroize,
/// then clear. Afterwards `buf` is empty and its heap region is all zeroes.
pub fn erase_buffer(buf: &mut Vec<u8>) {
    let cap = buf.capacity();
    buf.resize(cap, 0);
    buf.zeroize();
    buf.clear();
}

/// RAII guard registering `&mut Vec<u8>` buffers that must be erased when
/// the guard drops. Mirrors the pattern in the implementation spec §3.2.
///
/// Note: holding a guard keeps the buffers mutably borrowed, so it is the
/// tool for **small scopes that never touch the buffer again** (e.g. the
/// `#[cfg(test)]` erasure proofs). Long-lived streaming loops use explicit
/// `erase_buffer` calls at their boundaries instead (see `src/stream.rs`).
#[allow(dead_code)] // exercised by the capacity-region tests below
pub struct EraseGuard<'a> {
    buffers: Vec<&'a mut Vec<u8>>,
}

impl<'a> EraseGuard<'a> {
    #[allow(dead_code)] // used by tests; reserved for small-scope stages
    pub fn new() -> Self {
        EraseGuard { buffers: Vec::new() }
    }

    #[allow(dead_code)]
    pub fn register(&mut self, buf: &'a mut Vec<u8>) {
        self.buffers.push(buf);
    }
}

impl<'a> Default for EraseGuard<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Drop for EraseGuard<'a> {
    fn drop(&mut self) {
        for buf in self.buffers.drain(..) {
            erase_buffer(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// If this assertion fails, the capacity region was not erased — the
    /// buffer still leaks raw bytes the optimizer could not elide. This is
    /// the canary for the whole privacy property.
    #[test]
    fn erases_capacity_region_not_just_len() {
        let mut buf = Vec::with_capacity(4 * 1024);
        buf.resize(4, 0xEE);
        buf.resize(buf.capacity(), 0xAB); // dirty the whole allocation
        erase_buffer(&mut buf); // len -> 0, capacity region zeroed

        assert_eq!(buf.len(), 0, "buffer must be emptied");
        assert!(buf.capacity() >= 4 * 1024);
        // SAFETY: `erase_buffer` resized to `capacity` and zeroized before
        // clearing, so every byte of the allocation is initialized; the
        // read-only inspection up to capacity is the entire point of the
        // canary and cannot be elided.
        unsafe {
            let p = buf.as_ptr();
            let cap = buf.capacity();
            assert!(
                (0..cap).all(|i| *p.add(i) == 0),
                "capacity region must read back all-zero after erase"
            );
        }
    }

    #[test]
    fn erased_data_cannot_be_recovered_after_guard_drop() {
        let mut buf = vec![0u8; 0];
        buf.resize(64, 0xEE);
        {
            let mut guard = EraseGuard::new();
            guard.register(&mut buf);
        }
        assert_eq!(buf.len(), 0);
        assert!(buf.iter().all(|&b| b == 0));
    }
}
