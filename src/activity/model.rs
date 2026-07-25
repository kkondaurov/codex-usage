use crate::usage::UsageTotals;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActivityItem {
    pub(crate) id: String,
    pub(crate) turn_id: Option<String>,
    pub(crate) rollout_id: String,
    pub(crate) agent_run_id: Option<String>,
    pub(crate) agent_label: Option<String>,
    pub(crate) timestamp: String,
    pub(crate) kind: String,
    pub(crate) role: Option<String>,
    pub(crate) label: Option<String>,
    pub(crate) body: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) tool_name: Option<String>,
    pub(crate) duration_ms: Option<i64>,
    pub(crate) model: Option<String>,
    pub(crate) effort: Option<String>,
    pub(crate) has_details: bool,
    pub(crate) children: Vec<ActivityItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) child_page: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) child_page_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) child_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) child_has_more: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) child_next_cursor: Option<String>,
    pub(crate) usage: Option<UsageTotals>,
    pub(crate) counts: Option<ActivityCounts>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActivityCounts {
    pub(crate) model_calls: u64,
    pub(crate) tool_calls: u64,
    pub(crate) agent_runs: u64,
    pub(crate) reviews: u64,
    pub(crate) follow_ups: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActivityResponse {
    pub(crate) items: Vec<ActivityItem>,
    pub(crate) days: Vec<ActivityDaySummary>,
    pub(crate) page: u64,
    pub(crate) page_size: u64,
    pub(crate) total: u64,
    pub(crate) total_pages: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActivityDaySummary {
    pub(crate) date: String,
    pub(crate) duration_ms: u64,
    pub(crate) totals: UsageTotals,
}
