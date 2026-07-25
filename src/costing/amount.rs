use serde::{Serialize, Serializer};

const COST_UNITS_PER_USD: i128 = 1_000_000_000_000;

/// An exact USD amount represented in the smallest pricing unit used by this
/// application: one trillionth of a dollar. This is the natural result of
/// multiplying tokens by microdollars-per-million-token prices.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct UsdAmount(i128);

impl UsdAmount {
    pub const ZERO: Self = Self(0);

    pub fn from_cost_numerator(value: i128) -> Self {
        Self(value)
    }

    pub fn cost_numerator(self) -> i128 {
        self.0
    }

    pub fn decimal_string(self) -> String {
        let negative = self.0.is_negative();
        let magnitude = self.0.unsigned_abs();
        let scale = COST_UNITS_PER_USD as u128;
        let whole = magnitude / scale;
        let mut fraction = format!("{:012}", magnitude % scale);
        while fraction.len() > 2 && fraction.ends_with('0') {
            fraction.pop();
        }
        let sign = if negative { "-" } else { "" };
        format!("{sign}{whole}.{fraction}")
    }
}

impl Serialize for UsdAmount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.decimal_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_exact_canonical_decimal_strings() {
        for (numerator, expected) in [
            (0, "0.00"),
            (1, "0.000000000001"),
            (10_000_000_000, "0.01"),
            (100_000_000_000, "0.10"),
            (1_000_000_000_000, "1.00"),
            (1_234_567_890_123, "1.234567890123"),
            (-10_000_000_000, "-0.01"),
        ] {
            assert_eq!(
                UsdAmount::from_cost_numerator(numerator).decimal_string(),
                expected
            );
        }
    }

    #[test]
    fn serializes_as_a_json_string() {
        assert_eq!(
            serde_json::to_string(&UsdAmount::from_cost_numerator(100_000_000_001)).unwrap(),
            "\"0.100000000001\""
        );
    }
}
