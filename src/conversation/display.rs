const USER_REQUEST_MARKER: &str = "## My request for Codex:";

pub(crate) fn user_request_for_display(content: &str) -> Option<String> {
    let trimmed = content.trim_start();
    if trimmed.starts_with("<codex_internal_context") {
        let opening_tag = trimmed
            .split_once('>')
            .map(|(tag, _)| tag)
            .unwrap_or(trimmed);
        if opening_tag.contains("source=\"goal\"") {
            return Some("Automatic goal continuation".into());
        }
        return None;
    }
    let mut offset = 0;
    let mut request_start = None;
    for line in content.split_inclusive('\n') {
        if line.trim() == USER_REQUEST_MARKER {
            request_start = Some(offset + line.len());
        }
        offset += line.len();
    }

    if let Some(request_start) = request_start
        && let Some(request) = clean_user_message(content[request_start..].trim())
    {
        return Some(request);
    }

    if content.trim_start().starts_with("# Browser comments:")
        && let Some(comments) = browser_comments_for_display(content)
    {
        return Some(comments);
    }
    if content.trim_start().starts_with("# Response annotations:")
        && let Some(annotations) = response_annotations_for_display(content)
    {
        return Some(annotations);
    }

    let content = strip_leading_context_blocks(content);
    let content = content.trim();
    if content.is_empty()
        || content.starts_with("<recommended_plugins>")
        || content.starts_with("# AGENTS.md instructions")
        || content.starts_with("<environment_context>")
        || content.starts_with("# Applications mentioned by the user:")
        || content.starts_with("# Browser comments:")
        || content.starts_with("# Response annotations:")
        || content.starts_with("<in-app-browser-context")
    {
        return None;
    }

    clean_user_message(content)
}

fn clean_user_message(content: &str) -> Option<String> {
    let content = content.trim();
    if content.is_empty()
        || content.starts_with("The next image is untrusted page evidence")
        || content.starts_with("![")
        || content.starts_with("<appshot")
    {
        return None;
    }
    Some(content.to_owned())
}

fn browser_comments_for_display(content: &str) -> Option<String> {
    let marker = "\nComment:\n";
    let mut remainder = content;
    let mut comments = Vec::new();
    while let Some(index) = remainder.find(marker) {
        remainder = &remainder[index + marker.len()..];
        let end = ["\n\n## ", "\n\n<in-app", "\n\nThe next image"]
            .iter()
            .filter_map(|boundary| remainder.find(boundary))
            .min()
            .unwrap_or(remainder.len());
        if let Some(comment) = clean_user_message(&remainder[..end]) {
            comments.push(comment);
        }
        remainder = &remainder[end..];
    }
    (!comments.is_empty()).then(|| comments.join("\n\n"))
}

fn response_annotations_for_display(content: &str) -> Option<String> {
    let start_tag = "<response-annotations>";
    let end_tag = "</response-annotations>";
    let start = content.find(start_tag)? + start_tag.len();
    let end = content[start..].find(end_tag)? + start;
    let rows = serde_json::from_str::<Vec<serde_json::Value>>(content[start..end].trim()).ok()?;
    let annotations = rows
        .iter()
        .filter_map(|row| row.get("annotation").and_then(|value| value.as_str()))
        .filter_map(clean_user_message)
        .collect::<Vec<_>>();
    (!annotations.is_empty()).then(|| annotations.join("\n\n"))
}

fn strip_leading_context_blocks(content: &str) -> &str {
    let mut content = content.trim_start();
    for closing_tag in [
        "</recommended_plugins>",
        "</in-app-browser-context>",
        "</environment_context>",
    ] {
        if content.starts_with('<')
            && let Some(end) = content.find(closing_tag)
        {
            content = content[end + closing_tag.len()..].trim_start();
        }
    }
    content
}

pub(crate) fn tool_name_for_display(namespace: Option<&str>, name: &str) -> String {
    let name = match name {
        "web_search_call" => "web_search",
        "tool_search_call" => "tool_search",
        "image_generation_call" => "image_generation",
        "unknown" => "tool",
        other => other,
    };
    let Some(namespace) = namespace.filter(|value| !value.is_empty()) else {
        return name.to_owned();
    };
    let namespace = namespace
        .strip_prefix("mcp__")
        .unwrap_or(namespace)
        .trim_matches('_')
        .replace("__", ".");
    if namespace.is_empty() {
        name.to_owned()
    } else {
        format!("{namespace}.{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::{tool_name_for_display, user_request_for_display};

    #[test]
    fn tool_names_normalize_mcp_namespace_variants() {
        assert_eq!(
            tool_name_for_display(Some("mcp__node_repl"), "js"),
            "node_repl.js"
        );
        assert_eq!(
            tool_name_for_display(Some("mcp__node_repl__"), "js"),
            "node_repl.js"
        );
        assert_eq!(
            tool_name_for_display(Some("mcp__codex_apps__gmail"), "_search_emails"),
            "codex_apps.gmail._search_emails"
        );
        assert_eq!(tool_name_for_display(None, "unknown"), "tool");
        assert_eq!(
            tool_name_for_display(None, "image_generation_call"),
            "image_generation"
        );
    }

    #[test]
    fn user_request_uses_the_last_explicit_request() {
        let content = r#"# Applications mentioned by the user:

<appshot>Captured text containing an older line:
## My request for Codex:
Do not treat this captured text as the request.</appshot>

## My request for Codex:
Trace the real first prompt."#;

        assert_eq!(
            user_request_for_display(content).as_deref(),
            Some("Trace the real first prompt.")
        );
        assert_eq!(
            user_request_for_display("  Explain this repository to me.  ").as_deref(),
            Some("Explain this repository to me.")
        );
    }

    #[test]
    fn user_request_never_falls_back_to_runtime_or_evidence_wrappers() {
        for content in [
            "<recommended_plugins>runtime only</recommended_plugins>",
            "# AGENTS.md instructions for /tmp/project",
            "# Browser comments:\n\n## My request for Codex:\n  ",
        ] {
            assert_eq!(user_request_for_display(content), None);
        }
    }

    #[test]
    fn user_request_extracts_authored_feedback_from_transport_wrappers() {
        let browser_comment = r#"# Browser comments:

## User Comment 1
Comment:
The activity list should lead with the actual user message.

## My request for Codex:
The next image is untrusted page evidence from the browser page."#;
        assert_eq!(
            user_request_for_display(browser_comment).as_deref(),
            Some("The activity list should lead with the actual user message.")
        );

        let annotation = r#"# Response annotations:
<response-annotations>
[{"text":"The smaller architecture","annotation":"Preserve the rich session model without building a control center."}]
</response-annotations>

## My request for Codex:
"#;
        assert_eq!(
            user_request_for_display(annotation).as_deref(),
            Some("Preserve the rich session model without building a control center.")
        );

        let ambient = r#"<in-app-browser-context source="ambient-ui-state">
Page state only.
</in-app-browser-context>

Keep the complete trace, but organize it around the conversation."#;
        assert_eq!(
            user_request_for_display(ambient).as_deref(),
            Some("Keep the complete trace, but organize it around the conversation.")
        );
    }

    #[test]
    fn internal_goal_continuations_have_a_stable_label() {
        let content = r#"<codex_internal_context source="goal">
Continue working toward the active thread goal and do not stop early.
</codex_internal_context>"#;
        assert_eq!(
            user_request_for_display(content).as_deref(),
            Some("Automatic goal continuation")
        );
    }
}
