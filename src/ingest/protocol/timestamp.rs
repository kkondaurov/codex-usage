use crate::{MAX_PUBLIC_YEAR, MIN_PUBLIC_YEAR, calendar::canonical_utc_timestamp};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Datelike, Utc};

pub(in crate::ingest) fn canonical_source_timestamp(value: &str) -> Result<String> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid RFC3339 timestamp {value:?}"))?;
    let parsed = parsed.with_timezone(&Utc);
    if !(MIN_PUBLIC_YEAR..=MAX_PUBLIC_YEAR).contains(&parsed.year()) {
        return Err(anyhow!(
            "timestamp year must be between {MIN_PUBLIC_YEAR} and {MAX_PUBLIC_YEAR}"
        ));
    }
    Ok(canonical_utc_timestamp(parsed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_timestamp_normalization_is_exact_and_bounded() {
        assert_eq!(
            canonical_source_timestamp("2026-07-15T10:11:12.123+02:30").unwrap(),
            "2026-07-15T07:41:12.123000000Z"
        );
        assert_eq!(
            canonical_source_timestamp("2026-07-15T07:41:12Z").unwrap(),
            "2026-07-15T07:41:12.000000000Z"
        );
        assert!(canonical_source_timestamp("not-a-timestamp").is_err());
        assert!(canonical_source_timestamp("1969-12-31T23:59:59Z").is_err());
        assert!(canonical_source_timestamp("9999-01-01T00:00:00Z").is_err());
    }
}
