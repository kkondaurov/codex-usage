mod catalog;
mod list;
mod routes;
mod summary;

pub(crate) use routes::router;

// Cross-feature regression tests exercise the summary reader directly;
// production callers enter Sessions through `router` only.
#[cfg(test)]
pub(crate) use summary::read_summary_on;
