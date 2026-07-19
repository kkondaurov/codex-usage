use serde_json::Value;

const EMBEDDED_ATTACHMENT_PLACEHOLDER: &str = "[embedded attachment]";

pub(crate) fn redact_data_urls(value: &str) -> String {
    let ranges = data_url_ranges(value);
    if ranges.is_empty() {
        return value.to_owned();
    }
    let mut redacted = String::with_capacity(value.len().min(4096));
    let mut cursor = 0;
    for range in ranges {
        redacted.push_str(&value[cursor..range.start]);
        redacted.push_str(EMBEDDED_ATTACHMENT_PLACEHOLDER);
        cursor = range.end;
    }
    redacted.push_str(&value[cursor..]);
    redacted
}

pub(crate) fn serialize_redacted_json(value: &Value) -> serde_json::Result<String> {
    let mut redacted = value.clone();
    redact_json_value(&mut redacted);
    serde_json::to_string(&redacted)
}

fn redact_json_value(value: &mut Value) {
    match value {
        Value::String(value) => *value = redact_data_urls(value),
        Value::Array(values) => values.iter_mut().for_each(redact_json_value),
        Value::Object(values) => {
            let old = std::mem::take(values);
            for (key, mut value) in old {
                redact_json_value(&mut value);
                values.insert(redact_data_urls(&key), value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn data_url_ranges(value: &str) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut cursor = 0;
    while let Some(relative_start) = find_ascii_case_insensitive(&value[cursor..], "data:") {
        let start = cursor + relative_start;
        let Some(relative_comma) = value[start..].find(',') else {
            break;
        };
        let comma = start + relative_comma;
        if comma.saturating_sub(start) > 256
            || !value[start + 5..comma]
                .to_ascii_lowercase()
                .contains(";base64")
        {
            cursor = start + 5;
            continue;
        }
        let payload_start = comma + 1;
        let mut end = payload_start;
        for (offset, character) in value[payload_start..].char_indices() {
            if character.is_ascii_alphanumeric() || matches!(character, '+' | '/' | '=' | '_' | '-')
            {
                end = payload_start + offset + character.len_utf8();
            } else {
                break;
            }
        }
        if end == payload_start {
            cursor = payload_start;
            continue;
        }
        ranges.push(start..end);
        cursor = end;
    }
    ranges
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    haystack
        .as_bytes()
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_embedded_base64_data_urls_without_touching_other_data_urls() {
        assert_eq!(
            redact_data_urls(
                "before DATA:image/png;charset=utf-8;BASE64,aGVsbG8= after data:text/plain,hello"
            ),
            "before [embedded attachment] after data:text/plain,hello"
        );
    }

    #[test]
    fn serialized_redacted_json_remains_valid_and_redacts_keys_and_values() {
        let original = serde_json::json!({
            "nested": [
                {"image": "data:image/png;base64,VALUE_SENTINEL"},
                "ordinary text"
            ],
            "data:image/png;base64,KEY_SENTINEL": true
        });

        let encoded = serialize_redacted_json(&original).unwrap();
        let decoded: Value = serde_json::from_str(&encoded).unwrap();

        assert!(!encoded.to_ascii_lowercase().contains("data:image"));
        assert!(!encoded.contains("SENTINEL"));
        assert_eq!(
            decoded["nested"][0]["image"],
            Value::String("[embedded attachment]".into())
        );
        assert_eq!(decoded[EMBEDDED_ATTACHMENT_PLACEHOLDER], Value::Bool(true));
    }
}
