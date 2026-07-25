use super::catalog::{SessionRecord, read_session_on};
use crate::{
    conversation::display::{tool_name_for_display, user_request_for_display},
    costing::UsdAmount,
    usage::{
        RollupScope, TotalsScope, UsageAccumulator, UsageTotals, load_price_book_on,
        price_hourly_rollup_on, read_totals_on,
    },
};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashMap;

pub(crate) struct ModelUsageRecord {
    pub(crate) model: String,
    pub(crate) effort: Option<String>,
    pub(crate) input_tokens: u64,
    pub(crate) cached_input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) reasoning_tokens: u64,
    pub(crate) total_tokens: u64,
    pub(crate) cost_usd: Option<UsdAmount>,
    pub(crate) unpriced_tokens: u64,
}

pub(crate) struct AgentSummaryRecord {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) path: Option<String>,
    pub(crate) nickname: Option<String>,
    pub(crate) status: String,
    pub(crate) turn_count: u64,
    pub(crate) tool_count: u64,
    pub(crate) total_tokens: u64,
    pub(crate) cost_usd: Option<UsdAmount>,
    pub(crate) unpriced_tokens: u64,
}

pub(crate) struct ToolSummaryRecord {
    pub(crate) tool: String,
    pub(crate) count: u64,
    pub(crate) failed_count: u64,
    pub(crate) total_duration_ms: u64,
}

pub(crate) struct SessionDetailRecord {
    pub(crate) row: SessionRecord,
    pub(crate) cwd: Option<String>,
    pub(crate) source: Option<String>,
    pub(crate) first_prompt: Option<String>,
    pub(crate) latest_result: Option<String>,
    pub(crate) completed_at: Option<String>,
    pub(crate) status: String,
}

pub(crate) struct SessionSummaryRecord {
    pub(crate) session: SessionDetailRecord,
    pub(crate) totals: UsageTotals,
    pub(crate) models: Vec<ModelUsageRecord>,
    pub(crate) agents: Vec<AgentSummaryRecord>,
    pub(crate) tool_summary: Vec<ToolSummaryRecord>,
}

pub(crate) fn read_summary_on(
    connection: &Connection,
    thread_id: &str,
) -> Result<Option<SessionSummaryRecord>> {
    let Some(row) = read_session_on(connection, thread_id)? else {
        return Ok(None);
    };
    Ok(Some(SessionSummaryRecord {
        session: read_session_detail_on(connection, row)?,
        totals: read_totals_on(connection, None, None, TotalsScope::Thread { thread_id })?,
        models: read_model_usage_on(connection, thread_id)?,
        agents: read_agent_summary_on(connection, thread_id)?,
        tool_summary: read_tool_summary_on(connection, thread_id)?,
    }))
}

fn read_session_detail_on(
    connection: &Connection,
    row: SessionRecord,
) -> Result<SessionDetailRecord> {
    let root_rollout_id = read_session_root_rollout_id_on(connection, &row.id)?;
    let (cwd, source): (Option<String>, Option<String>) = connection.query_row(
        "SELECT cwd,COALESCE(thread_source,source) FROM threads WHERE id=?1",
        [&row.id],
        |value| Ok((value.get(0)?, value.get(1)?)),
    )?;
    let first_prompt = {
        let mut statement = connection.prepare(
            "SELECT content FROM messages
             WHERE thread_id=?1 AND rollout_id=?2 AND role='user'
             ORDER BY timestamp,source_line",
        )?;
        let mut messages = statement.query(params![&row.id, &root_rollout_id])?;
        let mut prompt = None;
        while let Some(message) = messages.next()? {
            let content: String = message.get(0)?;
            if let Some(content) = user_request_for_display(&content) {
                prompt = Some(content);
                break;
            }
        }
        prompt
    };
    let latest_message = connection
        .query_row(
            "SELECT content FROM messages
             WHERE thread_id=?1 AND rollout_id=?2 AND role='assistant'
             ORDER BY timestamp DESC,source_line DESC LIMIT 1",
            params![&row.id, &root_rollout_id],
            |value| value.get(0),
        )
        .optional()?;
    let latest_result = match latest_message {
        Some(message) => Some(message),
        None => connection
            .query_row(
                "SELECT last_agent_message FROM turns
                 WHERE thread_id=?1 AND rollout_id=?2
                   AND last_agent_message IS NOT NULL
                   AND trim(last_agent_message)<>''
                 ORDER BY COALESCE(completed_at,started_at) DESC LIMIT 1",
                params![&row.id, &root_rollout_id],
                |value| value.get(0),
            )
            .optional()?,
    };
    let completed_at = connection
        .query_row(
            "SELECT MAX(completed_at) FROM turns WHERE thread_id=?1",
            [&row.id],
            |value| value.get(0),
        )
        .optional()?
        .flatten();
    let status = connection
        .query_row(
            "SELECT status FROM agent_runs WHERE id=?1",
            [&row.id],
            |value| value.get(0),
        )
        .optional()?
        .unwrap_or_else(|| {
            if completed_at.is_some() {
                "completed".to_owned()
            } else {
                "running".to_owned()
            }
        });
    Ok(SessionDetailRecord {
        row,
        cwd,
        source,
        first_prompt,
        latest_result,
        completed_at,
        status,
    })
}

fn read_session_root_rollout_id_on(connection: &Connection, thread_id: &str) -> Result<String> {
    Ok(connection
        .query_row(
            "SELECT id FROM rollouts WHERE thread_id=?1
             ORDER BY CASE
                 WHEN id=?1 THEN 0
                 WHEN parent_rollout_id IS NULL AND parent_thread_id IS NULL THEN 1
                 ELSE 2 END,
                 started_at,id
             LIMIT 1",
            [thread_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .unwrap_or_else(|| thread_id.to_owned()))
}

fn read_model_usage_on(connection: &Connection, thread_id: &str) -> Result<Vec<ModelUsageRecord>> {
    let groups = {
        let mut statement = connection.prepare(
            "SELECT model,effort,
                    strftime('%Y-%m-%dT%H:00:00.000000000Z',timestamp) activity_hour,
                    COALESCE(SUM(input_tokens),0),
                    COALESCE(SUM(cached_input_tokens),0),
                    COALESCE(SUM(output_tokens),0),
                    COALESCE(SUM(reasoning_tokens),0),
                    COALESCE(SUM(total_tokens),0)
             FROM usage_facts
             WHERE thread_id=?1
             GROUP BY model,effort,activity_hour",
        )?;
        statement
            .query_map([thread_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?.max(0),
                    row.get::<_, i64>(4)?.max(0),
                    row.get::<_, i64>(5)?.max(0),
                    row.get::<_, i64>(6)?.max(0),
                    row.get::<_, i64>(7)?.max(0) as u64,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let price_book = load_price_book_on(connection)?;
    let mut totals = HashMap::<(String, Option<String>), UsageAccumulator>::new();
    for (
        model,
        effort,
        activity_hour,
        input_tokens,
        cached_input_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
    ) in groups
    {
        let (known_cost_numerator, unpriced_tokens) = price_hourly_rollup_on(
            connection,
            &price_book,
            RollupScope::Effort {
                thread_id,
                effort: effort.as_deref(),
            },
            &activity_hour,
            &model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens,
        )?;
        totals.entry((model, effort)).or_default().add_group(
            input_tokens as u64,
            cached_input_tokens as u64,
            output_tokens as u64,
            reasoning_tokens as u64,
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
        );
    }
    let mut usage = totals
        .into_iter()
        .map(|((model, effort), totals)| {
            let totals = totals.finish();
            ModelUsageRecord {
                model,
                effort,
                input_tokens: totals.input_tokens,
                cached_input_tokens: totals.cached_input_tokens,
                output_tokens: totals.output_tokens,
                reasoning_tokens: totals.reasoning_tokens,
                total_tokens: totals.total_tokens,
                cost_usd: totals.cost_usd,
                unpriced_tokens: totals.unpriced_tokens,
            }
        })
        .collect::<Vec<_>>();
    usage.sort_by(|left, right| {
        right
            .total_tokens
            .cmp(&left.total_tokens)
            .then_with(|| left.model.cmp(&right.model))
            .then_with(|| left.effort.cmp(&right.effort))
    });
    Ok(usage)
}

fn read_agent_totals_on(
    connection: &Connection,
    thread_id: &str,
) -> Result<HashMap<String, UsageAccumulator>> {
    let groups = {
        let mut statement = connection.prepare(
            "SELECT agent_run_id,
                    strftime('%Y-%m-%dT%H:00:00.000000000Z',timestamp) activity_hour,
                    model,COALESCE(SUM(input_tokens),0),
                    COALESCE(SUM(cached_input_tokens),0),
                    COALESCE(SUM(output_tokens),0),
                    COALESCE(SUM(reasoning_tokens),0),
                    COALESCE(SUM(total_tokens),0)
             FROM usage_facts
             WHERE thread_id=?1 AND agent_run_id IS NOT NULL
             GROUP BY agent_run_id,activity_hour,model",
        )?;
        statement
            .query_map([thread_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?.max(0),
                    row.get::<_, i64>(4)?.max(0),
                    row.get::<_, i64>(5)?.max(0),
                    row.get::<_, i64>(6)?.max(0),
                    row.get::<_, i64>(7)?.max(0) as u64,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let price_book = load_price_book_on(connection)?;
    let mut totals = HashMap::<String, UsageAccumulator>::new();
    for (
        agent_run_id,
        activity_hour,
        model,
        input_tokens,
        cached_input_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
    ) in groups
    {
        let (known_cost_numerator, unpriced_tokens) = price_hourly_rollup_on(
            connection,
            &price_book,
            RollupScope::Agent {
                thread_id,
                agent_run_id: &agent_run_id,
            },
            &activity_hour,
            &model,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens,
        )?;
        totals.entry(agent_run_id).or_default().add_group(
            input_tokens as u64,
            cached_input_tokens as u64,
            output_tokens as u64,
            reasoning_tokens as u64,
            total_tokens,
            known_cost_numerator,
            unpriced_tokens,
        );
    }
    Ok(totals)
}

fn read_agent_summary_on(
    connection: &Connection,
    thread_id: &str,
) -> Result<Vec<AgentSummaryRecord>> {
    let mut statement = connection.prepare(
        "SELECT a.id,a.agent_path,a.nickname,COALESCE(a.status,'running'),
                (SELECT COUNT(*) FROM turns tr WHERE tr.agent_run_id=a.id),
                (SELECT COUNT(*) FROM tool_calls tc WHERE tc.agent_run_id=a.id)
         FROM agent_runs a
         WHERE a.thread_id=?1 AND a.id<>a.thread_id
         ORDER BY a.started_at",
    )?;
    let rows = statement
        .query_map([thread_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?.max(0) as u64,
                row.get::<_, i64>(5)?.max(0) as u64,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut usage = read_agent_totals_on(connection, thread_id)?;
    Ok(rows
        .into_iter()
        .map(|(id, path, nickname, status, turn_count, tool_count)| {
            let totals = usage.remove(&id).unwrap_or_default().finish();
            let label = nickname
                .clone()
                .or_else(|| path.clone())
                .unwrap_or_else(|| "Primary agent".into());
            AgentSummaryRecord {
                id,
                label,
                path,
                nickname,
                status,
                turn_count,
                tool_count,
                total_tokens: totals.total_tokens,
                cost_usd: totals.cost_usd,
                unpriced_tokens: totals.unpriced_tokens,
            }
        })
        .collect())
}

fn read_tool_summary_on(
    connection: &Connection,
    thread_id: &str,
) -> Result<Vec<ToolSummaryRecord>> {
    let mut statement = connection.prepare(
        "SELECT namespace,name,COUNT(*),SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),
                COALESCE(SUM(duration_ms),0)
         FROM tool_calls WHERE thread_id=?1
         GROUP BY namespace,name",
    )?;
    let mut grouped: HashMap<String, ToolSummaryRecord> = HashMap::new();
    for row in statement.query_map([thread_id], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?.max(0) as u64,
            row.get::<_, i64>(3)?.max(0) as u64,
            row.get::<_, i64>(4)?.max(0) as u64,
        ))
    })? {
        let (namespace, name, count, failed_count, total_duration_ms) = row?;
        let tool = tool_name_for_display(namespace.as_deref(), &name);
        let entry = grouped.entry(tool.clone()).or_insert(ToolSummaryRecord {
            tool,
            count: 0,
            failed_count: 0,
            total_duration_ms: 0,
        });
        entry.count = entry.count.saturating_add(count);
        entry.failed_count = entry.failed_count.saturating_add(failed_count);
        entry.total_duration_ms = entry.total_duration_ms.saturating_add(total_duration_ms);
    }
    let mut tools = grouped.into_values().collect::<Vec<_>>();
    tools.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.tool.cmp(&right.tool))
    });
    tools.truncate(100);
    Ok(tools)
}
