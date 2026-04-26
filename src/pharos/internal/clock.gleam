//// Wall-clock time source.
////
//// A single home for "milliseconds since the Unix epoch", used to stamp
//// metrics and to age sliding-window samples. It wraps `erlang:system_time/1`
//// at the FFI boundary so the rest of the codebase depends on one function
//// rather than a raw external declared in whichever module first needed it.

/// Milliseconds since the Unix epoch (`erlang:system_time(millisecond)`).
@external(erlang, "pharos_ffi", "now_ms")
pub fn now_ms() -> Int
