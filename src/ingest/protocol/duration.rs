use serde_json::Value;

// Individual turns and tool calls can legitimately run for hours, but a
// single projected activity lasting longer than 30 days is corrupt metadata.
// Bounding each stored interval also keeps aggregate SQLite integer sums far
// away from overflow for any realistic local corpus.
pub(in crate::ingest) const MAX_STORED_DURATION_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

pub(in crate::ingest) fn duration_ms(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    value
        .as_i64()
        .or_else(|| {
            value.as_str().and_then(|text| {
                text.strip_suffix('s')?
                    .parse::<f64>()
                    .ok()
                    .map(|seconds| (seconds * 1_000.0).round() as i64)
            })
        })
        .or_else(|| {
            value
                .as_f64()
                .map(|seconds| (seconds * 1_000.0).round() as i64)
        })
        .or_else(|| {
            let seconds = value.get("secs")?.as_i64()?;
            let nanos = value.get("nanos").and_then(Value::as_i64).unwrap_or(0);
            let whole = seconds.saturating_mul(1_000);
            let fractional = nanos.max(0).saturating_add(999_999) / 1_000_000;
            Some(whole.saturating_add(fractional))
        })
        .and_then(|value| bounded_duration_ms(Some(value)))
}

pub(in crate::ingest) fn raw_duration_ms(value: Option<&Value>) -> Option<i64> {
    bounded_duration_ms(value.and_then(Value::as_i64))
}

pub(in crate::ingest) fn bounded_duration_ms(value: Option<i64>) -> Option<i64> {
    value.filter(|value| (0..=MAX_STORED_DURATION_MS).contains(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_duration_formats_share_one_exact_bounded_domain() {
        assert_eq!(duration_ms(Some(&serde_json::json!(123_i64))), Some(123));
        assert_eq!(
            duration_ms(Some(&serde_json::json!(1.234_f64))),
            Some(1_234)
        );
        assert_eq!(duration_ms(Some(&serde_json::json!("1.234s"))), Some(1_234));
        assert_eq!(
            duration_ms(Some(&serde_json::json!({"secs":1,"nanos":1}))),
            Some(1_001)
        );
        assert_eq!(duration_ms(Some(&serde_json::json!(-1_i64))), None);
        assert_eq!(
            raw_duration_ms(Some(&serde_json::json!(MAX_STORED_DURATION_MS))),
            Some(MAX_STORED_DURATION_MS)
        );
        assert_eq!(
            raw_duration_ms(Some(&serde_json::json!(MAX_STORED_DURATION_MS + 1))),
            None
        );
    }
}
