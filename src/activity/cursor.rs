use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActivityCollectionCursor {
    version: u8,
    thread_id: String,
    item_id: String,
    pub(crate) timestamp: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source_line: Option<i64>,
    pub(crate) sort_id: String,
}

pub(crate) fn encode_activity_collection_cursor(
    thread_id: &str,
    item_id: &str,
    timestamp: &str,
    source_line: Option<i64>,
    sort_id: &str,
) -> Result<String> {
    serde_json::to_string(&ActivityCollectionCursor {
        version: 1,
        thread_id: thread_id.to_owned(),
        item_id: item_id.to_owned(),
        timestamp: timestamp.to_owned(),
        source_line,
        sort_id: sort_id.to_owned(),
    })
    .context("failed to encode Activity collection cursor")
}

pub(crate) fn decode_activity_collection_cursor_for(
    value: &str,
    thread_id: &str,
    item_id: &str,
) -> Result<ActivityCollectionCursor> {
    if value.len() > 4_096 {
        return Err(anyhow!("Activity cursor is too long"));
    }
    let cursor: ActivityCollectionCursor =
        serde_json::from_str(value).context("invalid Activity collection cursor")?;
    if cursor.version != 1
        || cursor.thread_id != thread_id
        || cursor.item_id != item_id
        || cursor.timestamp.is_empty()
        || cursor
            .source_line
            .is_some_and(|source_line| source_line < 0)
        || cursor.sort_id.is_empty()
    {
        return Err(anyhow!("Activity cursor belongs to a different collection"));
    }
    Ok(cursor)
}

#[cfg(test)]
mod tests {
    use super::{decode_activity_collection_cursor_for, encode_activity_collection_cursor};

    #[test]
    fn collection_cursor_wire_contract_is_exact() {
        let legacy_item = "legacy:cursor-thread";
        let legacy = encode_activity_collection_cursor(
            "cursor-thread",
            legacy_item,
            "2026-07-01T00:00:00.000000000Z",
            Some(7),
            "legacy-message:message-7",
        )
        .unwrap();
        assert_eq!(
            legacy,
            r#"{"version":1,"threadId":"cursor-thread","itemId":"legacy:cursor-thread","timestamp":"2026-07-01T00:00:00.000000000Z","sourceLine":7,"sortId":"legacy-message:message-7"}"#
        );
        assert_eq!(
            decode_activity_collection_cursor_for(&legacy, "cursor-thread", legacy_item)
                .unwrap()
                .source_line,
            Some(7)
        );

        let review_item = "group:reviews:root";
        let review = encode_activity_collection_cursor(
            "cursor-thread",
            review_item,
            "2026-07-01T00:05:00.000000000Z",
            None,
            "review-004",
        )
        .unwrap();
        assert_eq!(
            review,
            r#"{"version":1,"threadId":"cursor-thread","itemId":"group:reviews:root","timestamp":"2026-07-01T00:05:00.000000000Z","sortId":"review-004"}"#
        );
        assert_eq!(
            decode_activity_collection_cursor_for(&review, "cursor-thread", review_item)
                .unwrap()
                .source_line,
            None
        );

        let old_legacy = r#"{"version":1,"threadId":"cursor-thread","itemId":"legacy:cursor-thread","timestamp":"2026-07-01T00:00:00.000000000Z","sortId":"legacy-message:message-7"}"#;
        assert_eq!(
            decode_activity_collection_cursor_for(old_legacy, "cursor-thread", legacy_item)
                .unwrap()
                .source_line,
            None
        );

        for (thread_id, item_id) in [
            ("other-thread", legacy_item),
            ("cursor-thread", "legacy:other-thread"),
        ] {
            assert_eq!(
                decode_activity_collection_cursor_for(&legacy, thread_id, item_id)
                    .unwrap_err()
                    .to_string(),
                "Activity cursor belongs to a different collection"
            );
        }
        assert_eq!(
            decode_activity_collection_cursor_for(&review, "cursor-thread", "group:agents:root",)
                .unwrap_err()
                .to_string(),
            "Activity cursor belongs to a different collection"
        );
    }
}
