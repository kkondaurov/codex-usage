use crate::MAX_USAGE_TOKENS_PER_FACT;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(in crate::ingest) struct TokenUsage {
    #[serde(default)]
    pub(in crate::ingest) input_tokens: u64,
    #[serde(default)]
    pub(in crate::ingest) cached_input_tokens: u64,
    #[serde(default)]
    pub(in crate::ingest) output_tokens: u64,
    #[serde(default)]
    pub(in crate::ingest) reasoning_output_tokens: u64,
    #[serde(default)]
    pub(in crate::ingest) total_tokens: u64,
}

impl TokenUsage {
    pub(in crate::ingest) fn is_zero(self) -> bool {
        self.input_tokens == 0 && self.output_tokens == 0 && self.total_tokens == 0
    }

    pub(in crate::ingest) fn decreased_from(self, previous: Self) -> bool {
        self.input_tokens < previous.input_tokens
            || self.cached_input_tokens < previous.cached_input_tokens
            || self.output_tokens < previous.output_tokens
            || self.reasoning_output_tokens < previous.reasoning_output_tokens
            || self.total_tokens < previous.total_tokens
    }

    pub(in crate::ingest) fn saturating_sub(self, previous: Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_sub(previous.input_tokens),
            cached_input_tokens: self
                .cached_input_tokens
                .saturating_sub(previous.cached_input_tokens),
            output_tokens: self.output_tokens.saturating_sub(previous.output_tokens),
            reasoning_output_tokens: self
                .reasoning_output_tokens
                .saturating_sub(previous.reasoning_output_tokens),
            total_tokens: self.total_tokens.saturating_sub(previous.total_tokens),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct TokenAccounting {
    pub(in crate::ingest) next_cumulative: TokenUsage,
    pub(in crate::ingest) usage: Option<TokenUsage>,
}

pub(in crate::ingest) fn decode_token_accounting(
    info: Option<&Value>,
    previous_cumulative: TokenUsage,
    native_started: bool,
    line: u64,
) -> Result<TokenAccounting> {
    let info = match info {
        Some(Value::Null) => {
            return Ok(TokenAccounting {
                next_cumulative: TokenUsage::default(),
                usage: None,
            });
        }
        None => &Value::Null,
        Some(info @ Value::Object(_)) => info,
        Some(_) => {
            return Err(anyhow!(
                "source line {line} has token_count.info with a non-object value"
            ));
        }
    };

    let total = parse_total_token_usage(info, line)?;
    if !native_started {
        return Ok(TokenAccounting {
            next_cumulative: total.unwrap_or(previous_cumulative),
            usage: None,
        });
    }

    let last = if total.is_some() && last_token_usage_is_total_only_hint(info) {
        None
    } else {
        parse_token_usage(info, "last_token_usage", line)?
    };
    let (next_cumulative, mut usage) = if let Some(current) = total {
        let delta = if current == previous_cumulative {
            TokenUsage::default()
        } else if current.decreased_from(previous_cumulative) {
            last.unwrap_or(current)
        } else {
            current.saturating_sub(previous_cumulative)
        };
        (current, delta)
    } else {
        (previous_cumulative, last.unwrap_or_default())
    };
    if usage.total_tokens == 0 {
        usage.total_tokens = usage
            .input_tokens
            .checked_add(usage.output_tokens)
            .ok_or_else(|| anyhow!("source line {line} has overflowing total_tokens"))?;
    }
    validate_token_usage(usage, true, "derived token usage", line)?;

    Ok(TokenAccounting {
        next_cumulative,
        usage: Some(usage),
    })
}

pub(in crate::ingest) fn checked_token_count(value: u64, field: &str, line: u64) -> Result<i64> {
    if value > MAX_USAGE_TOKENS_PER_FACT {
        return Err(anyhow!(
            "source line {line} has {field} above the supported {MAX_USAGE_TOKENS_PER_FACT}-token per-fact limit"
        ));
    }
    Ok(value as i64)
}

pub(in crate::ingest) fn parse_token_usage(
    info: &Value,
    field: &str,
    line: u64,
) -> Result<Option<TokenUsage>> {
    let Some(value) = info.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let mut usage = serde_json::from_value::<TokenUsage>(value.clone())
        .with_context(|| format!("source line {line} has invalid {field}"))?;
    let total_supplied = value.get("total_tokens").is_some();
    validate_token_usage(usage, total_supplied, field, line)?;
    if !total_supplied {
        usage.total_tokens = usage
            .input_tokens
            .checked_add(usage.output_tokens)
            .ok_or_else(|| anyhow!("source line {line} has overflowing {field}.total_tokens"))?;
    }
    Ok(Some(usage))
}

pub(in crate::ingest) fn parse_total_token_usage(
    info: &Value,
    line: u64,
) -> Result<Option<TokenUsage>> {
    let original_error = match parse_token_usage(info, "total_token_usage", line) {
        Ok(usage) => return Ok(usage),
        Err(error) => error,
    };
    let Some(Value::Object(value)) = info.get("total_token_usage") else {
        return Err(original_error);
    };
    let Some(context_window) = info.get("model_context_window").and_then(Value::as_u64) else {
        return Err(original_error);
    };
    if context_window == 0 {
        return Err(original_error);
    }
    let mut usage = serde_json::from_value::<TokenUsage>(Value::Object(value.clone()))
        .with_context(|| format!("source line {line} has invalid total_token_usage"))?;
    validate_token_usage(usage, false, "total_token_usage", line)?;
    let attributable_total = usage
        .input_tokens
        .checked_add(usage.output_tokens)
        .ok_or_else(|| {
            anyhow!("source line {line} has overflowing total_token_usage.total_tokens")
        })?;
    let total_with_context_window =
        attributable_total
            .checked_add(context_window)
            .ok_or_else(|| {
                anyhow!("source line {line} has overflowing total_token_usage.total_tokens")
            })?;
    if usage.total_tokens != total_with_context_window {
        return Err(original_error);
    }
    usage.total_tokens = attributable_total;
    Ok(Some(usage))
}

pub(in crate::ingest) fn last_token_usage_is_total_only_hint(info: &Value) -> bool {
    let Some(Value::Object(last)) = info.get("last_token_usage") else {
        return false;
    };
    [
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
    ]
    .into_iter()
    .all(|field| last.get(field).and_then(Value::as_u64) == Some(0))
        && last
            .get("total_tokens")
            .and_then(Value::as_u64)
            .is_some_and(|total| total > 0)
}

fn validate_token_usage(
    usage: TokenUsage,
    total_supplied: bool,
    field: &str,
    line: u64,
) -> Result<()> {
    if usage.cached_input_tokens > usage.input_tokens {
        return Err(anyhow!(
            "source line {line} has {field}.cached_input_tokens greater than input_tokens"
        ));
    }
    if usage.reasoning_output_tokens > usage.output_tokens {
        return Err(anyhow!(
            "source line {line} has {field}.reasoning_output_tokens greater than output_tokens"
        ));
    }
    let expected_total = usage
        .input_tokens
        .checked_add(usage.output_tokens)
        .ok_or_else(|| anyhow!("source line {line} has overflowing {field}.total_tokens"))?;
    if total_supplied && usage.total_tokens != expected_total {
        return Err(anyhow!(
            "source line {line} has {field}.total_tokens inconsistent with input_tokens + output_tokens"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn token_usage_delta_semantics_are_componentwise() {
        let previous = usage(10, 10, 10, 10);
        assert!(!previous.decreased_from(previous));
        for current in [
            TokenUsage {
                input_tokens: 9,
                ..previous
            },
            TokenUsage {
                cached_input_tokens: 9,
                ..previous
            },
            TokenUsage {
                output_tokens: 9,
                ..previous
            },
            TokenUsage {
                reasoning_output_tokens: 9,
                ..previous
            },
            TokenUsage {
                total_tokens: 9,
                ..previous
            },
        ] {
            assert!(current.decreased_from(previous));
        }
        assert_eq!(
            usage(5, 1, 7, 2).saturating_sub(usage(3, 4, 9, 1)),
            TokenUsage {
                input_tokens: 2,
                cached_input_tokens: 0,
                output_tokens: 0,
                reasoning_output_tokens: 1,
                total_tokens: 0,
            }
        );
    }

    #[test]
    fn token_accounting_reset_duplicate_growth_and_decrease_are_exact() {
        let previous = usage(10, 4, 3, 2);
        assert_eq!(
            decode_token_accounting(Some(&Value::Null), previous, true, 7).unwrap(),
            TokenAccounting {
                next_cumulative: TokenUsage::default(),
                usage: None,
            }
        );

        let duplicate = serde_json::json!({"total_token_usage":previous});
        assert_eq!(
            decode_token_accounting(Some(&duplicate), previous, true, 7)
                .unwrap()
                .usage,
            Some(TokenUsage::default())
        );

        let current = usage(14, 5, 8, 3);
        let growth = serde_json::json!({"total_token_usage":current});
        assert_eq!(
            decode_token_accounting(Some(&growth), previous, true, 7)
                .unwrap()
                .usage,
            Some(current.saturating_sub(previous))
        );

        let reset = usage(2, 1, 1, 0);
        let precise_last = usage(1, 0, 1, 0);
        let decrease = serde_json::json!({
            "total_token_usage":reset,
            "last_token_usage":precise_last
        });
        assert_eq!(
            decode_token_accounting(Some(&decrease), previous, true, 7).unwrap(),
            TokenAccounting {
                next_cumulative: reset,
                usage: Some(precise_last),
            }
        );
    }

    #[test]
    fn token_snapshot_shapes_and_bounds_are_preserved() {
        let derived = serde_json::json!({"last_token_usage":{
            "input_tokens":10,"cached_input_tokens":4,
            "output_tokens":3,"reasoning_output_tokens":2
        }});
        assert_eq!(
            parse_token_usage(&derived, "last_token_usage", 7).unwrap(),
            Some(usage(10, 4, 3, 2))
        );
        let invalid = serde_json::json!({"last_token_usage":{
            "input_tokens":2,"cached_input_tokens":3,
            "output_tokens":0,"reasoning_output_tokens":0,"total_tokens":2
        }});
        assert!(parse_token_usage(&invalid, "last_token_usage", 7).is_err());
        assert!(checked_token_count(MAX_USAGE_TOKENS_PER_FACT + 1, "input_tokens", 7).is_err());
    }
}
