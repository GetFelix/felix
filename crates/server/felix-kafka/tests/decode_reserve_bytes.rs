//! A peer-claimed array count must not reserve more memory than the frame's
//! bytes could back. Capping the count at the bytes left is not enough: an
//! element one byte long on the wire can be tens of bytes in memory.
//!
//! Its own test binary because it swaps the global allocator to see the largest
//! single allocation, and a second test running alongside would muddy that.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::MetadataRequest;
use kafka_protocol::protocol::Decodable;

struct LargestAllocation;

static WATCHING: AtomicBool = AtomicBool::new(false);
static LARGEST: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for LargestAllocation {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if WATCHING.load(Ordering::Relaxed) {
            LARGEST.fetch_max(layout.size(), Ordering::Relaxed);
        }
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `alloc` above, which is the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: LargestAllocation = LargestAllocation;

#[test]
fn an_array_reservation_is_bounded_by_the_frame_bytes_not_its_count() {
    const FRAME: usize = 1 << 20;
    // Metadata v1: a topics array claiming as many entries as there are bytes
    // after it, then bytes that fail the first entry (a -2 name length).
    let mut body = BytesMut::with_capacity(4 + FRAME);
    body.put_i32(FRAME as i32);
    body.put_bytes(0xfe, FRAME);
    let mut body = body.freeze();

    WATCHING.store(true, Ordering::Relaxed);
    let decoded = MetadataRequest::decode(&mut body, 1);
    WATCHING.store(false, Ordering::Relaxed);

    assert!(decoded.is_err(), "the first topic entry is malformed");
    let largest = LARGEST.load(Ordering::Relaxed);
    assert!(
        largest <= FRAME,
        "decoding a {FRAME}-byte frame reserved {largest} bytes at once"
    );
}
