mod activity_index;
pub mod api;
pub mod config;
pub mod db;
pub mod db_executor;
pub mod fixed_price;
pub mod ingest;
pub mod manual_pricing;
pub mod model;
pub mod money;
pub mod pricing;
mod process_lock;
mod redaction;

pub(crate) const MIN_PUBLIC_YEAR: i32 = 1970;
pub(crate) const MAX_PUBLIC_YEAR: i32 = 9998;
pub(crate) const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
// Keep every SQL fixed-point multiplication inside SQLite's signed 64-bit
// integer domain. Four billion tokens is over 10,000 times the largest fact in
// the current corpus and still leaves room for a $1,000 / million-token price.
pub(crate) const MAX_USAGE_TOKENS_PER_FACT: u64 = 4_000_000_000;
