use crate::redaction::redact_data_urls;
use serde_json::Value;

pub(in crate::ingest) fn extract_content(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(extract_content)
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(map) => [
            "text",
            "input_text",
            "output_text",
            "summary_text",
            "content",
        ]
        .into_iter()
        .find_map(|key| map.get(key).map(extract_content))
        .unwrap_or_default(),
        _ => String::new(),
    }
}

pub(in crate::ingest) fn has_omitted_attachment(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(has_omitted_attachment),
        Value::Object(map) => {
            let attachment_type = map.get("type").and_then(Value::as_str).is_some_and(|kind| {
                matches!(
                    kind,
                    "attachment"
                        | "file"
                        | "image"
                        | "input_audio"
                        | "input_file"
                        | "input_image"
                        | "output_audio"
                        | "output_file"
                        | "output_image"
                )
            });
            attachment_type
                || ["attachment", "attachments", "file_url", "image_url"]
                    .iter()
                    .any(|key| map.contains_key(*key))
                || map.values().any(has_omitted_attachment)
        }
        _ => false,
    }
}

fn value_to_string(value: &Value) -> String {
    if let Some(value) = value.as_str() {
        value.to_owned()
    } else {
        serde_json::to_string(value).unwrap_or_default()
    }
}

pub(in crate::ingest) fn value_to_text(value: &Value) -> Option<String> {
    let text = extract_content(value);
    if text.is_empty() {
        Some(value_to_string(value)).filter(|value| value != "null" && value != "{}")
    } else {
        Some(text)
    }
}

pub(in crate::ingest) fn is_turn_abort_envelope(content: &str) -> bool {
    let content = content.trim();
    content.starts_with("<turn_aborted>") && content.contains("</turn_aborted>")
}

pub(in crate::ingest) fn is_transport_context_envelope(content: &str) -> bool {
    let content = content.trim_start();
    content.starts_with("# AGENTS.md instructions")
        || content.starts_with("<environment_context>")
        || is_recommended_plugins_transport_bundle(content)
}

fn is_recommended_plugins_transport_bundle(content: &str) -> bool {
    let Some(after_opening) = content.strip_prefix("<recommended_plugins>") else {
        return false;
    };
    let Some((_, after_plugins)) = after_opening.split_once("</recommended_plugins>") else {
        return false;
    };

    let mut remainder = after_plugins.trim();
    if remainder.is_empty() {
        return true;
    }
    if remainder.starts_with("# AGENTS.md instructions") {
        let Some((_, after_agents)) = remainder.split_once("</INSTRUCTIONS>") else {
            return false;
        };
        remainder = after_agents.trim();
    }
    if remainder.starts_with("<environment_context>") {
        let Some((_, after_environment)) = remainder.split_once("</environment_context>") else {
            return false;
        };
        remainder = after_environment.trim();
    }

    remainder.is_empty()
}

pub(in crate::ingest) fn redact_and_bound(value: &str, max_chars: usize) -> String {
    let value = redact_data_urls(value);
    let mut chars = value.chars();
    let mut bounded = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        bounded.push('…');
    }
    bounded
}

pub(in crate::ingest) fn normalized_metadata_value(
    value: Option<&str>,
    max_chars: usize,
) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| redact_and_bound(value, max_chars))
}

pub(in crate::ingest) fn compact_title(value: &str) -> String {
    let value = redact_data_urls(value);
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = collapsed.chars();
    let title: String = chars.by_ref().take(180).collect();
    if chars.next().is_some() {
        format!("{title}…")
    } else {
        title
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_content_extraction_preserves_precedence_and_array_order() {
        let content = serde_json::json!([
            {"type":"input_text","text":"first"},
            {"content":[{"output_text":"second"}, null, 42]},
            {"text":"", "input_text":"not selected"},
            true
        ]);

        assert_eq!(extract_content(&content), "first\nsecond");
        assert_eq!(extract_content(&serde_json::json!({"unknown":"value"})), "");
    }

    #[test]
    fn attachment_detection_recognizes_supported_types_keys_and_nesting() {
        assert!(has_omitted_attachment(&serde_json::json!([
            {"content":[{"type":"input_image"}]}
        ])));
        assert!(has_omitted_attachment(&serde_json::json!({
            "nested":{"file_url":"https://example.test/file"}
        })));
        assert!(!has_omitted_attachment(&serde_json::json!({
            "type":"input_text", "text":"ordinary"
        })));
    }

    #[test]
    fn value_text_prefers_extracted_content_then_uses_exact_json_fallbacks() {
        assert_eq!(
            value_to_text(&serde_json::json!({"output_text":"rendered"})),
            Some("rendered".into())
        );
        assert_eq!(
            value_to_text(&serde_json::json!({"value":7})),
            Some(r#"{"value":7}"#.into())
        );
        assert_eq!(value_to_text(&Value::Null), None);
        assert_eq!(value_to_text(&serde_json::json!({})), None);
        assert_eq!(value_to_string(&serde_json::json!("plain")), "plain");
    }

    #[test]
    fn content_redaction_and_metadata_bounds_count_characters() {
        assert_eq!(
            redact_and_bound("before data:image/png;base64,private after", 1_000),
            "before [embedded attachment] after"
        );
        assert_eq!(redact_and_bound("abcdef", 3), "abc…");
        assert_eq!(redact_and_bound("ééé", 2), "éé…");
        assert_eq!(
            normalized_metadata_value(Some("  branch-name  "), 64),
            Some("branch-name".into())
        );
        assert_eq!(normalized_metadata_value(Some("  "), 64), None);
        assert_eq!(normalized_metadata_value(None, 64), None);
    }

    #[test]
    fn abort_and_transport_classification_are_strict() {
        assert!(is_turn_abort_envelope(
            " <turn_aborted>cancelled</turn_aborted> "
        ));
        assert!(!is_turn_abort_envelope(
            "prefix <turn_aborted>x</turn_aborted>"
        ));
        assert!(is_transport_context_envelope(
            "  <environment_context>local</environment_context>"
        ));
        assert!(is_transport_context_envelope(
            "# AGENTS.md instructions\n<INSTRUCTIONS>rules</INSTRUCTIONS>"
        ));
        assert!(!is_transport_context_envelope("ordinary user prompt"));
    }

    #[test]
    fn recommended_plugin_bundle_accepts_only_the_runtime_envelope_sequence() {
        assert!(is_recommended_plugins_transport_bundle(
            "<recommended_plugins>none</recommended_plugins>\n\
             # AGENTS.md instructions\n<INSTRUCTIONS>rules</INSTRUCTIONS>\n\
             <environment_context>local</environment_context>"
        ));
        assert!(is_recommended_plugins_transport_bundle(
            "<recommended_plugins>none</recommended_plugins>"
        ));
        assert!(!is_recommended_plugins_transport_bundle(
            "<recommended_plugins>none</recommended_plugins>\nactual prompt"
        ));
        assert!(!is_recommended_plugins_transport_bundle(
            " <recommended_plugins>none</recommended_plugins>"
        ));
    }

    #[test]
    fn compact_titles_redact_collapse_and_bound_the_projected_label() {
        assert_eq!(compact_title("  first\n\tsecond  "), "first second");
        assert_eq!(
            compact_title("before data:image/png;base64,private after"),
            "before [embedded attachment] after"
        );
        assert_eq!(
            compact_title(&"é".repeat(181)),
            format!("{}…", "é".repeat(180))
        );
    }
}
