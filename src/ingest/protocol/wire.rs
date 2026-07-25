use serde_json::Value;

/// A borrowed view over one source record.
///
/// Wire decoding stays on this side of the protocol boundary. Projection code
/// receives owned intents and never needs to inspect the source JSON.
#[derive(Clone, Copy, Debug)]
pub(in crate::ingest) struct WireRecord<'a> {
    value: &'a Value,
}

impl<'a> WireRecord<'a> {
    pub(in crate::ingest) fn new(value: &'a Value) -> Self {
        Self { value }
    }

    pub(in crate::ingest) fn outer_type(self) -> Option<&'a str> {
        self.value.get("type").and_then(Value::as_str)
    }

    pub(in crate::ingest) fn payload(self) -> Option<&'a Value> {
        self.value.get("payload")
    }

    pub(in crate::ingest) fn payload_type(self) -> Option<&'a str> {
        self.payload()
            .and_then(|payload| payload.get("type"))
            .and_then(Value::as_str)
    }

    pub(in crate::ingest) fn explicit_timestamp(self) -> Option<&'a str> {
        self.value
            .get("timestamp")
            .and_then(Value::as_str)
            .or_else(|| {
                self.payload()
                    .and_then(|payload| payload.get("timestamp"))
                    .and_then(Value::as_str)
            })
    }

    pub(in crate::ingest) fn payload_field(self, field: &str) -> Option<&'a Value> {
        self.payload().and_then(|payload| payload.get(field))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_record_borrows_one_value_and_preserves_timestamp_precedence() {
        let value = serde_json::json!({
            "type":"event_msg",
            "timestamp":"2026-07-25T10:00:00Z",
            "payload":{
                "type":"token_count",
                "timestamp":"2026-07-25T11:00:00Z",
                "info":null
            }
        });
        let record = WireRecord::new(&value);

        assert_eq!(record.outer_type(), Some("event_msg"));
        assert_eq!(record.payload_type(), Some("token_count"));
        assert_eq!(record.explicit_timestamp(), Some("2026-07-25T10:00:00Z"));
        assert!(record.payload_field("info").is_some_and(Value::is_null));
    }
}
