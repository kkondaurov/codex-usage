use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct PriceInterval {
    effective_from: String,
    effective_to: Option<String>,
    input_microusd_per_million: i64,
    cached_input_microusd_per_million: Option<i64>,
    output_microusd_per_million: i64,
}

impl PriceInterval {
    pub(crate) fn new(
        effective_from: String,
        effective_to: Option<String>,
        input_microusd_per_million: i64,
        cached_input_microusd_per_million: Option<i64>,
        output_microusd_per_million: i64,
    ) -> Self {
        Self {
            effective_from,
            effective_to,
            input_microusd_per_million,
            cached_input_microusd_per_million,
            output_microusd_per_million,
        }
    }

    pub(crate) fn cost_numerator(
        &self,
        uncached_input_tokens: i64,
        cached_input_tokens: i64,
        output_tokens: i64,
    ) -> i128 {
        i128::from(uncached_input_tokens)
            .saturating_mul(i128::from(self.input_microusd_per_million))
            .saturating_add(
                i128::from(cached_input_tokens).saturating_mul(i128::from(
                    self.cached_input_microusd_per_million
                        .unwrap_or(self.input_microusd_per_million),
                )),
            )
            .saturating_add(
                i128::from(output_tokens)
                    .saturating_mul(i128::from(self.output_microusd_per_million)),
            )
    }

    fn contains(&self, timestamp: &str) -> bool {
        self.effective_from.as_str() <= timestamp
            && self
                .effective_to
                .as_deref()
                .is_none_or(|effective_to| effective_to > timestamp)
    }

    fn is_outside(&self, first_timestamp: &str, last_timestamp: &str) -> bool {
        self.effective_from.as_str() > last_timestamp
            || self
                .effective_to
                .as_deref()
                .is_some_and(|effective_to| effective_to <= first_timestamp)
    }

    fn has_boundary_within(&self, first_timestamp: &str, last_timestamp: &str) -> bool {
        (self.effective_from.as_str() > first_timestamp
            && self.effective_from.as_str() <= last_timestamp)
            || self.effective_to.as_deref().is_some_and(|effective_to| {
                effective_to > first_timestamp && effective_to <= last_timestamp
            })
    }
}

#[derive(Debug)]
pub(crate) struct PriceBook {
    aliases: HashMap<String, String>,
    ledger: HashMap<String, Vec<PriceInterval>>,
}

impl PriceBook {
    pub(crate) fn new(
        aliases: HashMap<String, String>,
        ledger: HashMap<String, Vec<PriceInterval>>,
    ) -> Self {
        Self { aliases, ledger }
    }

    pub(crate) fn price_at(&self, model: &str, timestamp: &str) -> Option<(usize, &PriceInterval)> {
        let priced_model = self.canonical_model(model);
        self.ledger
            .get(priced_model)?
            .iter()
            .enumerate()
            .rev()
            .find(|(_, price)| price.contains(timestamp))
    }

    pub(crate) fn group_has_no_price(
        &self,
        model: &str,
        first_timestamp: &str,
        last_timestamp: &str,
    ) -> bool {
        let priced_model = self.canonical_model(model);
        self.ledger.get(priced_model).is_none_or(|model_prices| {
            model_prices
                .iter()
                .all(|price| price.is_outside(first_timestamp, last_timestamp))
        })
    }

    pub(crate) fn group_has_price_boundary(
        &self,
        model: &str,
        first_timestamp: &str,
        last_timestamp: &str,
    ) -> bool {
        let priced_model = self.canonical_model(model);
        self.ledger.get(priced_model).is_some_and(|model_prices| {
            model_prices
                .iter()
                .any(|price| price.has_boundary_within(first_timestamp, last_timestamp))
        })
    }

    fn canonical_model<'a>(&'a self, model: &'a str) -> &'a str {
        self.aliases.get(model).map(String::as_str).unwrap_or(model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const START: &str = "2026-01-01T00:00:00.000000000Z";
    const ONE: &str = "2026-01-01T01:00:00.000000000Z";
    const TWO: &str = "2026-01-01T02:00:00.000000000Z";

    fn interval(
        effective_from: &str,
        effective_to: Option<&str>,
        input: i64,
        cached: Option<i64>,
        output: i64,
    ) -> PriceInterval {
        PriceInterval::new(
            effective_from.to_owned(),
            effective_to.map(str::to_owned),
            input,
            cached,
            output,
        )
    }

    #[test]
    fn aliases_resolve_to_the_canonical_ledger() {
        let book = PriceBook::new(
            HashMap::from([("observed".to_owned(), "canonical".to_owned())]),
            HashMap::from([(
                "canonical".to_owned(),
                vec![interval(START, None, 3, None, 7)],
            )]),
        );

        let (_, price) = book.price_at("observed", START).unwrap();
        assert_eq!(price.cost_numerator(2, 0, 1), 13);
    }

    #[test]
    fn effective_end_is_exclusive_and_gaps_remain_unpriced() {
        let book = PriceBook::new(
            HashMap::new(),
            HashMap::from([(
                "model".to_owned(),
                vec![
                    interval(START, Some(ONE), 1, None, 1),
                    interval(TWO, None, 2, None, 2),
                ],
            )]),
        );

        assert_eq!(book.price_at("model", START).unwrap().0, 0);
        assert!(book.price_at("model", ONE).is_none());
        assert_eq!(book.price_at("model", TWO).unwrap().0, 1);
        assert!(book.group_has_no_price("model", ONE, ONE));
        assert!(!book.group_has_no_price("model", ONE, TWO));
        assert!(book.group_has_price_boundary("model", START, ONE));
        assert!(book.group_has_price_boundary("model", ONE, TWO));
    }

    #[test]
    fn later_ledger_entries_keep_their_interval_identity_and_precedence() {
        let book = PriceBook::new(
            HashMap::new(),
            HashMap::from([(
                "model".to_owned(),
                vec![
                    interval(START, None, 1, None, 1),
                    interval(START, None, 9, None, 9),
                ],
            )]),
        );

        let (index, selected) = book.price_at("model", TWO).unwrap();
        assert_eq!(index, 1);
        assert_eq!(selected.cost_numerator(1, 0, 0), 9);
        assert!(!book.group_has_price_boundary("model", START, TWO));
    }

    #[test]
    fn cached_input_falls_back_to_the_normal_input_rate() {
        let fallback = interval(START, None, 5, None, 7);
        let explicit = interval(START, None, 5, Some(2), 7);

        assert_eq!(fallback.cost_numerator(2, 3, 4), 53);
        assert_eq!(explicit.cost_numerator(2, 3, 4), 44);
    }

    #[test]
    fn scalar_cost_arithmetic_saturates_instead_of_wrapping() {
        let price = interval(START, None, i64::MAX, Some(i64::MAX), i64::MAX);
        assert_eq!(
            price.cost_numerator(i64::MAX, i64::MAX, i64::MAX),
            i128::MAX
        );
    }
}
