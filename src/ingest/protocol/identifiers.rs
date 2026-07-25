use crate::redaction::redact_data_urls;
use anyhow::{Result, anyhow};

pub(in crate::ingest) const PROJECTED_IDENTIFIER_CHARS: usize = 256;

pub(in crate::ingest) fn normalized_relational_identifier(
    value: Option<&str>,
    label: &str,
) -> Result<Option<String>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.chars().count() > PROJECTED_IDENTIFIER_CHARS {
        return Err(anyhow!(
            "{label} exceeds the {PROJECTED_IDENTIFIER_CHARS}-character identifier limit"
        ));
    }
    if value.chars().any(char::is_control) || redact_data_urls(value) != value {
        return Err(anyhow!("{label} contains invalid identifier content"));
    }
    Ok(Some(value.to_owned()))
}

pub(in crate::ingest) fn required_metadata_identifier(
    value: Option<&str>,
    label: &str,
) -> Result<String> {
    normalized_relational_identifier(value, label)?
        .ok_or_else(|| anyhow!("first session_meta has no {label}"))
}

pub(in crate::ingest) fn looks_like_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
}

pub(in crate::ingest) fn is_owner_native_turn(owner_id: &str, turn_id: &str) -> bool {
    if let Some(owner_timestamp) = uuid7_timestamp(owner_id) {
        // A replayed legacy turn can use a random UUID and compare greater than
        // a time-ordered UUIDv7 by accident. Only UUIDv7 turns participate in
        // the chronological fork boundary for UUIDv7 rollouts. Compare the
        // timestamp field rather than random suffixes so same-millisecond IDs
        // remain correctly ordered as native.
        uuid7_timestamp(turn_id).is_some_and(|turn_timestamp| turn_timestamp >= owner_timestamp)
    } else {
        !turn_id.is_empty()
    }
}

fn uuid7_timestamp(value: &str) -> Option<u64> {
    looks_like_uuid7(value).then_some(())?;
    let high = u64::from_str_radix(&value[..8], 16).ok()?;
    let low = u64::from_str_radix(&value[9..13], 16).ok()?;
    Some((high << 16) | low)
}

fn looks_like_uuid7(value: &str) -> bool {
    let bytes = value.as_bytes();
    looks_like_uuid(value)
        && bytes[14] == b'7'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b' | b'A' | b'B')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relational_identifiers_trim_count_characters_and_reject_unsafe_content() {
        assert_eq!(
            normalized_relational_identifier(None, "turn id").unwrap(),
            None
        );
        assert_eq!(
            normalized_relational_identifier(Some("   "), "turn id").unwrap(),
            None
        );
        assert_eq!(
            normalized_relational_identifier(Some("  turn-id  "), "turn id").unwrap(),
            Some("turn-id".into())
        );

        let maximum_unicode_identifier = "é".repeat(PROJECTED_IDENTIFIER_CHARS);
        assert_eq!(
            normalized_relational_identifier(Some(&maximum_unicode_identifier), "turn id")
                .unwrap()
                .unwrap()
                .chars()
                .count(),
            PROJECTED_IDENTIFIER_CHARS
        );
        assert!(
            normalized_relational_identifier(
                Some(&"é".repeat(PROJECTED_IDENTIFIER_CHARS + 1)),
                "turn id"
            )
            .is_err()
        );
        assert!(normalized_relational_identifier(Some("bad\nturn"), "turn id").is_err());
        assert!(
            normalized_relational_identifier(Some("data:image/png;base64,private"), "turn id")
                .is_err()
        );
    }

    #[test]
    fn relational_identifier_errors_preserve_the_current_contract() {
        let oversized = "x".repeat(PROJECTED_IDENTIFIER_CHARS + 1);
        assert_eq!(
            normalized_relational_identifier(Some(&oversized), "turn id")
                .unwrap_err()
                .to_string(),
            "turn id exceeds the 256-character identifier limit"
        );
        assert_eq!(
            normalized_relational_identifier(Some("bad\tturn"), "turn id")
                .unwrap_err()
                .to_string(),
            "turn id contains invalid identifier content"
        );
        assert_eq!(
            required_metadata_identifier(Some("   "), "rollout id")
                .unwrap_err()
                .to_string(),
            "first session_meta has no rollout id"
        );
        assert_eq!(
            required_metadata_identifier(Some(" rollout-id "), "rollout id").unwrap(),
            "rollout-id"
        );
    }

    #[test]
    fn uuid_shape_requires_canonical_hyphens_and_hexadecimal_bytes() {
        for value in [
            "019f64aa-ffff-7fff-bfff-ffffffffffff",
            "019F64AA-FFFF-7FFF-BFFF-FFFFFFFFFFFF",
        ] {
            assert!(looks_like_uuid(value));
        }

        for value in [
            "",
            "019f64aa-ffff-7fff-bfff-fffffffffff",
            "019f64aaffff-7fff-bfff-ffffffffffff",
            "019f64aa-ffff-7zzz-bfff-ffffffffffff",
            "019f64aa-ffff-7fff-bfff-ffffffffffé",
        ] {
            assert!(!looks_like_uuid(value), "unexpected UUID match: {value}");
        }
    }

    #[test]
    fn uuid7_detection_requires_version_and_rfc_variant() {
        assert!(looks_like_uuid7("019f64aa-ffff-7000-8000-000000000000"));
        assert!(looks_like_uuid7("019F64AA-FFFF-7000-B000-000000000000"));
        assert!(!looks_like_uuid7("392fc773-e404-46d6-8764-595914ed82f6"));
        assert!(!looks_like_uuid7("019f64aa-ffff-7000-0000-000000000000"));
        assert!(!looks_like_uuid7("019f64aa-ffff-7zzz-8000-000000000000"));
    }

    #[test]
    fn uuid7_timestamp_uses_only_the_embedded_milliseconds() {
        assert_eq!(
            uuid7_timestamp("019f64aa-ffff-7000-8000-000000000000"),
            Some((0x019f64aa_u64 << 16) | 0xffff)
        );
        assert_eq!(
            uuid7_timestamp("019F64AA-FFFF-7FFF-BFFF-FFFFFFFFFFFF"),
            Some((0x019f64aa_u64 << 16) | 0xffff)
        );
        assert_eq!(
            uuid7_timestamp("392fc773-e404-46d6-8764-595914ed82f6"),
            None
        );
    }

    #[test]
    fn owner_native_turn_uses_uuid7_time_boundary_and_legacy_fallback() {
        let owner = "019f64aa-ffff-7fff-bfff-ffffffffffff";
        assert!(is_owner_native_turn(
            owner,
            "019f64aa-ffff-7000-8000-000000000000"
        ));
        assert!(is_owner_native_turn(
            owner,
            "019F64AB-0000-7000-8000-000000000000"
        ));
        assert!(!is_owner_native_turn(
            owner,
            "019f64aa-fffe-7fff-bfff-ffffffffffff"
        ));
        assert!(!is_owner_native_turn(
            owner,
            "392fc773-e404-46d6-8764-595914ed82f6"
        ));
        assert!(!is_owner_native_turn(
            owner,
            "019f64ab-0000-7000-0000-000000000000"
        ));

        assert!(is_owner_native_turn("legacy-owner", "legacy-turn"));
        assert!(!is_owner_native_turn("legacy-owner", ""));
    }
}
