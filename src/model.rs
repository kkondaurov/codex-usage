use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub total_tokens: u64,
    pub blended_tokens: u64,
    pub cost_usd: Option<f64>,
    #[serde(skip)]
    pub known_cost_usd: f64,
    pub unpriced_tokens: u64,
    pub pricing_complete: bool,
}

impl Totals {
    pub fn finish(mut self) -> Self {
        self.blended_tokens = self
            .input_tokens
            .saturating_sub(self.cached_input_tokens)
            .saturating_add(self.cached_input_tokens / 10)
            .saturating_add(self.output_tokens);
        self.pricing_complete = self.unpriced_tokens == 0;
        self.cost_usd = self.pricing_complete.then_some(self.known_cost_usd);
        self
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TokenUsage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub cached_input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub reasoning_output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
}

impl TokenUsage {
    pub fn is_zero(self) -> bool {
        self.input_tokens == 0 && self.output_tokens == 0 && self.total_tokens == 0
    }

    pub fn decreased_from(self, previous: Self) -> bool {
        self.input_tokens < previous.input_tokens
            || self.cached_input_tokens < previous.cached_input_tokens
            || self.output_tokens < previous.output_tokens
            || self.reasoning_output_tokens < previous.reasoning_output_tokens
            || self.total_tokens < previous.total_tokens
    }

    pub fn saturating_sub(self, previous: Self) -> Self {
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
