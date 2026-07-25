use crate::costing::{PriceBook, UsdAmount};
use serde::Serialize;

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsageTotals {
    pub(crate) input_tokens: u64,
    pub(crate) cached_input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) reasoning_tokens: u64,
    pub(crate) total_tokens: u64,
    pub(crate) blended_tokens: u64,
    pub(crate) cost_usd: Option<UsdAmount>,
    #[serde(skip)]
    pub(crate) known_cost_numerator: i128,
    pub(crate) unpriced_tokens: u64,
    pub(crate) pricing_complete: bool,
}

impl UsageTotals {
    pub(crate) fn finish(mut self) -> Self {
        self.blended_tokens = self
            .input_tokens
            .saturating_sub(self.cached_input_tokens)
            .saturating_add(self.cached_input_tokens / 10)
            .saturating_add(self.output_tokens);
        self.pricing_complete = self.unpriced_tokens == 0;
        self.cost_usd = self
            .pricing_complete
            .then_some(UsdAmount::from_cost_numerator(self.known_cost_numerator));
        self
    }
}

#[derive(Clone, Default)]
pub(crate) struct UsageAccumulator {
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    total_tokens: u64,
    known_cost_numerator: i128,
    unpriced_tokens: u64,
}

impl UsageAccumulator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_group(
        &mut self,
        input_tokens: u64,
        cached_input_tokens: u64,
        output_tokens: u64,
        reasoning_tokens: u64,
        total_tokens: u64,
        known_cost_numerator: i128,
        unpriced_tokens: u64,
    ) {
        self.input_tokens = self.input_tokens.saturating_add(input_tokens);
        self.cached_input_tokens = self.cached_input_tokens.saturating_add(cached_input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(output_tokens);
        self.reasoning_tokens = self.reasoning_tokens.saturating_add(reasoning_tokens);
        self.total_tokens = self.total_tokens.saturating_add(total_tokens);
        self.known_cost_numerator = self
            .known_cost_numerator
            .saturating_add(known_cost_numerator);
        self.unpriced_tokens = self.unpriced_tokens.saturating_add(unpriced_tokens);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.add_group(
            other.input_tokens,
            other.cached_input_tokens,
            other.output_tokens,
            other.reasoning_tokens,
            other.total_tokens,
            other.known_cost_numerator,
            other.unpriced_tokens,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_fact(
        &mut self,
        price_book: &PriceBook,
        timestamp: &str,
        model: &str,
        input_tokens: i64,
        cached_input_tokens: i64,
        output_tokens: i64,
        reasoning_tokens: i64,
        total_tokens: i64,
    ) {
        let input_tokens = input_tokens.max(0);
        let cached_input_tokens = cached_input_tokens.max(0).min(input_tokens);
        let output_tokens = output_tokens.max(0);
        let reasoning_tokens = reasoning_tokens.max(0);
        let total_tokens = total_tokens.max(0) as u64;
        let (known_cost_numerator, unpriced_tokens) =
            price_book
                .price_at(model, timestamp)
                .map_or((0, total_tokens), |(_, price)| {
                    (
                        price.cost_numerator(
                            input_tokens - cached_input_tokens,
                            cached_input_tokens,
                            output_tokens,
                        ),
                        0,
                    )
                });
        self.add_group(
            input_tokens as u64,
            cached_input_tokens as u64,
            output_tokens as u64,
            reasoning_tokens as u64,
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
        );
    }

    pub(crate) fn total_tokens(&self) -> u64 {
        self.total_tokens
    }

    pub(crate) fn known_cost_numerator(&self) -> i128 {
        self.known_cost_numerator
    }

    pub(crate) fn unpriced_tokens(&self) -> u64 {
        self.unpriced_tokens
    }

    pub(crate) fn finish(self) -> UsageTotals {
        UsageTotals {
            input_tokens: self.input_tokens,
            cached_input_tokens: self.cached_input_tokens,
            output_tokens: self.output_tokens,
            reasoning_tokens: self.reasoning_tokens,
            total_tokens: self.total_tokens,
            known_cost_numerator: self.known_cost_numerator,
            unpriced_tokens: self.unpriced_tokens,
            ..UsageTotals::default()
        }
        .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::costing::PriceInterval;
    use serde_json::json;
    use std::collections::HashMap;

    const START: &str = "2026-01-01T00:00:00.000000000Z";

    fn price_book(cached_input_price: Option<i64>) -> PriceBook {
        PriceBook::new(
            HashMap::new(),
            HashMap::from([(
                "priced".to_owned(),
                vec![PriceInterval::new(
                    START.to_owned(),
                    None,
                    3,
                    cached_input_price,
                    7,
                )],
            )]),
        )
    }

    #[test]
    fn finish_preserves_the_serialized_contract_and_exact_cost() {
        let totals = UsageTotals {
            input_tokens: 20,
            cached_input_tokens: 10,
            output_tokens: 3,
            reasoning_tokens: 2,
            total_tokens: 25,
            known_cost_numerator: 123,
            ..UsageTotals::default()
        }
        .finish();

        assert_eq!(
            serde_json::to_value(totals).unwrap(),
            json!({
                "inputTokens": 20,
                "cachedInputTokens": 10,
                "outputTokens": 3,
                "reasoningTokens": 2,
                "totalTokens": 25,
                "blendedTokens": 14,
                "costUsd": "0.000000000123",
                "unpricedTokens": 0,
                "pricingComplete": true
            })
        );
    }

    #[test]
    fn add_fact_clamps_cached_input_and_uses_the_input_rate_as_fallback() {
        let mut totals = UsageAccumulator::default();
        totals.add_fact(&price_book(None), START, "priced", 5, 9, 2, -4, 7);
        let totals = totals.finish();

        assert_eq!(totals.input_tokens, 5);
        assert_eq!(totals.cached_input_tokens, 5);
        assert_eq!(totals.output_tokens, 2);
        assert_eq!(totals.reasoning_tokens, 0);
        assert_eq!(totals.known_cost_numerator, 29);
        assert_eq!(
            totals.cost_usd.unwrap().cost_numerator(),
            totals.known_cost_numerator
        );
    }

    #[test]
    fn any_unpriced_tokens_suppress_the_partial_cost() {
        let book = price_book(Some(1));
        let mut totals = UsageAccumulator::default();
        totals.add_fact(&book, START, "priced", 2, 1, 1, 0, 3);
        totals.add_fact(&book, START, "unknown", 4, 0, 1, 0, 5);
        let totals = totals.finish();

        assert_eq!(totals.known_cost_numerator, 11);
        assert_eq!(totals.unpriced_tokens, 5);
        assert!(!totals.pricing_complete);
        assert_eq!(totals.cost_usd, None);
    }

    #[test]
    fn group_and_merge_arithmetic_saturates_instead_of_wrapping() {
        let mut totals = UsageAccumulator::default();
        totals.add_group(
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            i128::MAX,
            0,
        );
        let mut extra = UsageAccumulator::default();
        extra.add_group(1, 1, 1, 1, 1, 1, 0);
        totals.merge(extra);
        let totals = totals.finish();

        assert_eq!(totals.input_tokens, u64::MAX);
        assert_eq!(totals.cached_input_tokens, u64::MAX);
        assert_eq!(totals.output_tokens, u64::MAX);
        assert_eq!(totals.reasoning_tokens, u64::MAX);
        assert_eq!(totals.total_tokens, u64::MAX);
        assert_eq!(totals.known_cost_numerator, i128::MAX);
        assert_eq!(totals.blended_tokens, u64::MAX);
    }
}
