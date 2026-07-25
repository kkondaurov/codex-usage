use super::super::{
    intent::{CursorTransition, DecodedUsageRecord, UsageIntent},
    state::CursorState,
    timestamp::canonical_source_timestamp,
    tokens::decode_token_accounting,
    wire::WireRecord,
};
use anyhow::{Result, anyhow};
use serde_json::Value;

pub(in crate::ingest) fn decode_usage_record(
    state: &CursorState,
    line: u64,
    value: &Value,
) -> Result<Option<DecodedUsageRecord>> {
    let wire = WireRecord::new(value);
    if wire.outer_type() != Some("event_msg") || wire.payload_type() != Some("token_count") {
        return Ok(None);
    }

    let timestamp = match wire.explicit_timestamp() {
        Some(timestamp) => canonical_source_timestamp(timestamp)?,
        None => state
            .last_timestamp
            .clone()
            .ok_or_else(|| anyhow!("source line {line} has no timestamp and no prior timestamp"))?,
    };
    let accounting = decode_token_accounting(
        wire.payload_field("info"),
        state.cumulative,
        state.native_started,
        line,
    )?;

    Ok(Some(DecodedUsageRecord {
        source_line: line,
        transition: CursorTransition {
            last_timestamp: timestamp.clone(),
            next_cumulative: accounting.next_cumulative,
        },
        timestamp,
        intent: UsageIntent {
            usage: accounting.usage,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::super::super::tokens::TokenUsage;
    use super::*;

    fn usage(input: u64, cached: u64, output: u64, reasoning: u64) -> TokenUsage {
        TokenUsage {
            input_tokens: input,
            cached_input_tokens: cached,
            output_tokens: output,
            reasoning_output_tokens: reasoning,
            total_tokens: input + output,
        }
    }

    fn state(
        last_timestamp: Option<&str>,
        cumulative: TokenUsage,
        native_started: bool,
    ) -> CursorState {
        CursorState {
            last_timestamp: last_timestamp.map(str::to_owned),
            cumulative,
            native_started,
            ..CursorState::default()
        }
    }

    fn token_record(timestamp: Option<&str>, info: Value) -> Value {
        let mut value = serde_json::json!({
            "type":"event_msg",
            "payload":{"type":"token_count","info":info}
        });
        if let Some(timestamp) = timestamp {
            value["timestamp"] = Value::String(timestamp.to_owned());
        }
        value
    }

    fn decoded_usage(record: &DecodedUsageRecord) -> Option<TokenUsage> {
        record.intent.usage
    }

    #[test]
    fn non_usage_records_are_not_claimed() {
        let value = serde_json::json!({
            "type":"event_msg",
            "payload":{"type":"task_started"}
        });

        assert_eq!(
            decode_usage_record(&CursorState::default(), 7, &value).unwrap(),
            None
        );
    }

    #[test]
    fn explicit_offsets_are_canonical_and_outer_timestamp_wins() {
        let value = serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T12:15:30.125+02:30",
            "payload":{
                "type":"token_count",
                "timestamp":"2026-07-25T12:15:31Z",
                "info":null
            }
        });
        let decoded = decode_usage_record(&CursorState::default(), 17, &value)
            .unwrap()
            .unwrap();

        assert_eq!(decoded.source_line, 17);
        assert_eq!(decoded.timestamp, "2026-07-25T09:45:30.125000000Z");
        assert_eq!(decoded.transition.last_timestamp, decoded.timestamp);
    }

    #[test]
    fn missing_explicit_timestamp_uses_prior_or_reports_the_exact_boundary_error() {
        let value = token_record(None, Value::Null);
        let prior = "2026-07-25T09:45:30.125000000Z";
        let decoded =
            decode_usage_record(&state(Some(prior), TokenUsage::default(), true), 23, &value)
                .unwrap()
                .unwrap();

        assert_eq!(decoded.timestamp, prior);
        assert_eq!(decoded.transition.last_timestamp, prior);

        let error = decode_usage_record(&CursorState::default(), 23, &value).unwrap_err();
        assert_eq!(
            error.to_string(),
            "source line 23 has no timestamp and no prior timestamp"
        );
    }

    #[test]
    fn null_resets_cumulative_but_remains_an_explicit_usage_intent() {
        let previous = usage(10, 4, 3, 2);
        let value = token_record(Some("2026-07-25T10:00:00Z"), Value::Null);
        let decoded = decode_usage_record(&state(None, previous, true), 31, &value)
            .unwrap()
            .unwrap();

        assert_eq!(decoded_usage(&decoded), None);
        assert_eq!(decoded.transition.next_cumulative, TokenUsage::default());
    }

    #[test]
    fn pre_native_snapshots_inherit_or_replace_cumulative_without_emitting_usage() {
        let previous = usage(10, 4, 3, 2);
        let inherited = token_record(
            Some("2026-07-25T10:00:00Z"),
            serde_json::json!({"last_token_usage":usage(2, 1, 1, 0)}),
        );
        let decoded = decode_usage_record(&state(None, previous, false), 41, &inherited)
            .unwrap()
            .unwrap();
        assert_eq!(decoded_usage(&decoded), None);
        assert_eq!(decoded.transition.next_cumulative, previous);

        let replacement = usage(20, 8, 5, 3);
        let snapshot = token_record(
            Some("2026-07-25T10:00:01Z"),
            serde_json::json!({"total_token_usage":replacement}),
        );
        let decoded = decode_usage_record(&state(None, previous, false), 42, &snapshot)
            .unwrap()
            .unwrap();
        assert_eq!(decoded_usage(&decoded), None);
        assert_eq!(decoded.transition.next_cumulative, replacement);
    }

    #[test]
    fn duplicate_growth_and_decrease_accounting_is_preserved_exactly() {
        let previous = usage(10, 4, 3, 2);
        let timestamp = Some("2026-07-25T10:00:00Z");

        let duplicate = token_record(timestamp, serde_json::json!({"total_token_usage":previous}));
        let decoded = decode_usage_record(&state(None, previous, true), 51, &duplicate)
            .unwrap()
            .unwrap();
        assert_eq!(decoded_usage(&decoded), Some(TokenUsage::default()));
        assert_eq!(decoded.transition.next_cumulative, previous);

        let current = usage(14, 5, 8, 3);
        let growth = token_record(timestamp, serde_json::json!({"total_token_usage":current}));
        let decoded = decode_usage_record(&state(None, previous, true), 52, &growth)
            .unwrap()
            .unwrap();
        assert_eq!(
            decoded_usage(&decoded),
            Some(current.saturating_sub(previous))
        );
        assert_eq!(decoded.transition.next_cumulative, current);

        let reset = usage(2, 1, 1, 0);
        let precise_last = usage(1, 0, 1, 0);
        let decrease = token_record(
            timestamp,
            serde_json::json!({
                "total_token_usage":reset,
                "last_token_usage":precise_last
            }),
        );
        let decoded = decode_usage_record(&state(None, previous, true), 53, &decrease)
            .unwrap()
            .unwrap();
        assert_eq!(decoded_usage(&decoded), Some(precise_last));
        assert_eq!(decoded.transition.next_cumulative, reset);
    }

    #[test]
    fn malformed_info_is_rejected_without_leaking_wire_values_into_the_intent() {
        let value = token_record(Some("2026-07-25T10:00:00Z"), Value::String("bad".into()));
        let error = decode_usage_record(&CursorState::default(), 61, &value).unwrap_err();

        assert_eq!(
            error.to_string(),
            "source line 61 has token_count.info with a non-object value"
        );
    }
}
