use crate::{
    costing::UsdAmount,
    usage::{TotalsScope, read_all_time_totals_on},
};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};

pub(crate) struct SessionRecord {
    pub(crate) id: String,
    pub(crate) started_at: String,
    pub(crate) last_event_at: String,
    pub(crate) title: String,
    pub(crate) project: String,
    pub(crate) branch: Option<String>,
    pub(crate) message_count: u64,
    pub(crate) turn_count: u64,
    pub(crate) agent_count: u64,
    pub(crate) tool_count: u64,
    pub(crate) total_tokens: u64,
    pub(crate) cost_usd: Option<UsdAmount>,
    pub(crate) unpriced_tokens: u64,
    pub(crate) lifetime_cost_usd: Option<UsdAmount>,
    pub(crate) lifetime_unpriced_tokens: u64,
}

struct SessionHeader {
    id: String,
    started_at: String,
    last_event_at: String,
    title: String,
    project: String,
    branch: Option<String>,
    message_count: u64,
    turn_count: u64,
    agent_count: u64,
    tool_count: u64,
}

pub(crate) fn read_session_on(connection: &Connection, id: &str) -> Result<Option<SessionRecord>> {
    let header = connection
        .query_row(
            "SELECT t.id,t.started_at,t.last_event_at,COALESCE(t.title,'Untitled session'),
                COALESCE(t.project,'—'),t.branch,
                (SELECT COUNT(*) FROM messages m WHERE m.thread_id=t.id),
                (SELECT COUNT(*) FROM turns tr WHERE tr.thread_id=t.id),
                (SELECT COUNT(*) FROM agent_runs a WHERE a.thread_id=t.id AND a.id<>a.thread_id),
                (SELECT COUNT(*) FROM tool_calls tc WHERE tc.thread_id=t.id)
             FROM threads t WHERE t.id=?1 AND (
                EXISTS(SELECT 1 FROM events e WHERE e.thread_id=t.id)
                OR EXISTS(SELECT 1 FROM usage_facts u WHERE u.thread_id=t.id)
                OR EXISTS(SELECT 1 FROM messages m WHERE m.thread_id=t.id))",
            [id],
            |row| {
                Ok(SessionHeader {
                    id: row.get(0)?,
                    started_at: row.get(1)?,
                    last_event_at: row.get(2)?,
                    title: row.get(3)?,
                    project: row.get(4)?,
                    branch: row.get(5)?,
                    message_count: row.get::<_, i64>(6)?.max(0) as u64,
                    turn_count: row.get::<_, i64>(7)?.max(0) as u64,
                    agent_count: row.get::<_, i64>(8)?.max(0) as u64,
                    tool_count: row.get::<_, i64>(9)?.max(0) as u64,
                })
            },
        )
        .optional()?;
    let Some(header) = header else {
        return Ok(None);
    };
    let totals = read_all_time_totals_on(
        connection,
        TotalsScope::Thread {
            thread_id: header.id.as_str(),
        },
    )?;
    Ok(Some(SessionRecord {
        id: header.id,
        started_at: header.started_at,
        last_event_at: header.last_event_at,
        title: header.title,
        project: header.project,
        branch: header.branch,
        message_count: header.message_count,
        turn_count: header.turn_count,
        agent_count: header.agent_count,
        tool_count: header.tool_count,
        total_tokens: totals.total_tokens,
        cost_usd: totals.cost_usd,
        unpriced_tokens: totals.unpriced_tokens,
        lifetime_cost_usd: totals.cost_usd,
        lifetime_unpriced_tokens: totals.unpriced_tokens,
    }))
}

pub(crate) fn read_projects_on(connection: &Connection) -> Result<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT DISTINCT project FROM threads t
         WHERE project IS NOT NULL AND project<>'' AND (
            EXISTS(SELECT 1 FROM events e WHERE e.thread_id=t.id)
            OR EXISTS(SELECT 1 FROM usage_facts u WHERE u.thread_id=t.id)
            OR EXISTS(SELECT 1 FROM messages m WHERE m.thread_id=t.id)
         ) ORDER BY project COLLATE NOCASE",
    )?;
    Ok(statement
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}
