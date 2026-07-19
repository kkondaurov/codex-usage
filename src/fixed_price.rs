use anyhow::{Result, bail};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use serde_json::Number;
use std::str::FromStr;

const MICROS_PER_USD: i64 = 1_000_000;
const PER_TOKEN_TO_MICROUSD_PER_MILLION: i64 = 1_000_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriceMicros(i64);

impl PriceMicros {
    pub fn from_raw(value: i64) -> Result<Self> {
        if value < 0 {
            bail!("price cannot be negative");
        }
        Ok(Self(value))
    }

    pub fn from_per_million_text(value: &str) -> Result<Self> {
        let decimal = parse_decimal(value)?;
        if decimal.is_sign_negative() {
            bail!("price cannot be negative");
        }
        let scaled = decimal * Decimal::from(MICROS_PER_USD);
        if !scaled.fract().is_zero() {
            bail!("price supports at most six decimal places");
        }
        let raw = scaled
            .to_i64()
            .ok_or_else(|| anyhow::anyhow!("price is too large"))?;
        Self::from_raw(raw)
    }

    pub fn from_per_token_number(value: &Number) -> Result<Self> {
        let decimal = parse_decimal(&value.to_string())?;
        if decimal.is_sign_negative() {
            bail!("price cannot be negative");
        }
        let scaled = (decimal * Decimal::from(PER_TOKEN_TO_MICROUSD_PER_MILLION))
            .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero);
        let raw = scaled
            .to_i64()
            .ok_or_else(|| anyhow::anyhow!("price is too large"))?;
        Self::from_raw(raw)
    }

    pub fn raw(self) -> i64 {
        self.0
    }

    pub fn decimal_string(self) -> String {
        let whole = self.0 / MICROS_PER_USD;
        let mut fraction = format!("{:06}", self.0 % MICROS_PER_USD);
        while fraction.len() > 2 && fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{whole}.{fraction}")
    }
}

fn parse_decimal(value: &str) -> Result<Decimal> {
    let value = value.trim();
    if value.is_empty() {
        bail!("price is required");
    }
    Decimal::from_str(value)
        .or_else(|_| Decimal::from_scientific(value))
        .map_err(|_| anyhow::anyhow!("invalid decimal price"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn per_million_prices_round_trip_as_canonical_decimal_strings() {
        for (text, raw, canonical) in [
            ("0.40", 400_000, "0.40"),
            ("0.10", 100_000, "0.10"),
            ("1.60", 1_600_000, "1.60"),
            ("0.025", 25_000, "0.025"),
            ("30", 30_000_000, "30.00"),
            ("0.000001", 1, "0.000001"),
        ] {
            let price = PriceMicros::from_per_million_text(text).unwrap();
            assert_eq!(price.raw(), raw);
            assert_eq!(price.decimal_string(), canonical);
        }
    }

    #[test]
    fn manual_prices_reject_negative_or_overprecise_values() {
        assert!(PriceMicros::from_per_million_text("-0.1").is_err());
        assert!(PriceMicros::from_per_million_text("0.0000001").is_err());
    }

    #[test]
    fn per_token_json_numbers_convert_without_binary_floating_point() {
        let number = json!(0.0000004).as_number().unwrap().clone();
        let price = PriceMicros::from_per_token_number(&number).unwrap();
        assert_eq!(price.raw(), 400_000);
        assert_eq!(price.decimal_string(), "0.40");
    }
}
