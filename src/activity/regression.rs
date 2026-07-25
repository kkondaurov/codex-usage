#![cfg(test)]

use super::{
    cursor::{decode_activity_collection_cursor_for, encode_activity_collection_cursor},
    detail::{
        read_cursor_page_on as query_activity_detail_cursor_page_on,
        read_default_on as query_activity_detail_on,
        read_numeric_page_on as query_activity_detail_page_on,
    },
    previews::{
        ACTIVITY_MESSAGE_PARSE_BYTES, ACTIVITY_PREVIEW_CHARS,
        query_activity_child_previews_cursor_page, query_activity_child_previews_page,
        query_legacy_message_child_rows, read_legacy_root as query_legacy_activity_item,
    },
    root_page::{
        activity_day_window, query_activity_day_summaries_batched,
        read_page_on as query_activity_on,
    },
    routes::{ActivityDetailQuery, session_activity_detail},
};
use crate::{
    calendar::local_midnight,
    usage::{TotalsScope, load_price_book_on, read_totals_on},
};
use crate::{
    sessions::read_summary_on,
    storage::{Db, StorageExecutor},
    web::ReadRuntime,
};
use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
};
use chrono::NaiveDate;
use rusqlite::params;
use std::{
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

static TRACE_LOCK: Mutex<()> = Mutex::new(());
static QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);

fn count_query(sql: &str) {
    let sql = sql.trim_start();
    if sql.starts_with("SELECT") || sql.starts_with("WITH") {
        QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
    }
}

fn seed_activity_roots(connection: &rusqlite::Connection, thread_id: &str, roots: usize) {
    connection
        .execute_batch(&format!(
            "INSERT INTO threads(id,title,started_at,last_event_at)
             VALUES('{thread_id}','Query budget','2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T01:00:00.000000000Z');
             INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
             VALUES('{thread_id}','{thread_id}','2026-07-01T00:00:00.000000000Z',
                    '2026-07-01T01:00:00.000000000Z',0);"
        ))
        .unwrap();
    for index in 0..roots {
        let minute = index + 1;
        connection
            .execute_batch(&format!(
                "INSERT INTO turns(
                    id,thread_id,rollout_id,started_at,completed_at,status,duration_ms
                 ) VALUES(
                    'root-{index}','{thread_id}','{thread_id}',
                    '2026-07-01T00:{minute:02}:00.000000000Z',
                    '2026-07-01T00:{minute:02}:30.000000000Z','completed',30000
                 );
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,role,body,native
                 ) VALUES(
                    'user-{index}','{thread_id}','{thread_id}','root-{index}',
                    '2026-07-01T00:{minute:02}:00.000000000Z',1,'user','user',
                    'Request {index}',1
                 );
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES(
                    'usage-{index}','{thread_id}','{thread_id}','root-{index}',
                    '2026-07-01T00:{minute:02}:10.000000000Z',2,'gpt-5.5',
                    100,50,10,2,110,1
                 );"
            ))
            .unwrap();
    }
}

fn seed_activity_descendants(
    connection: &rusqlite::Connection,
    thread_id: &str,
    start: usize,
    count: usize,
) {
    for index in start..start + count {
        connection
            .execute_batch(&format!(
                "INSERT INTO rollouts(
                    id,thread_id,parent_rollout_id,parent_thread_id,started_at,last_event_at,archived
                 ) VALUES(
                    'agent-{index}','{thread_id}','{thread_id}','{thread_id}',
                    '2026-07-01T02:{index:02}:00.000000000Z',
                    '2026-07-01T02:{index:02}:30.000000000Z',0
                 );
                 INSERT INTO agent_runs(
                    id,thread_id,rollout_id,parent_rollout_id,nickname,started_at,completed_at,status
                 ) VALUES(
                    'agent-{index}','{thread_id}','agent-{index}','{thread_id}','Agent {index}',
                    '2026-07-01T02:{index:02}:00.000000000Z',
                    '2026-07-01T02:{index:02}:30.000000000Z','completed'
                 );
                 INSERT INTO turns(
                    id,thread_id,rollout_id,agent_run_id,started_at,completed_at,status,duration_ms
                 ) VALUES(
                    'child-{index}','{thread_id}','agent-{index}','agent-{index}',
                    '2026-07-01T02:{index:02}:00.000000000Z',
                    '2026-07-01T02:{index:02}:30.000000000Z','completed',30000
                 );
                 INSERT INTO events(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,kind,payload_json,native
                 ) VALUES(
                    'spawn-{index}','{thread_id}','{thread_id}','root-0',
                    '2026-07-01T00:01:01.000000000Z',{index},'subagent',
                    '{{\"agent_thread_id\":\"agent-{index}\"}}',1
                 );
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,turn_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,
                    total_tokens,native
                 ) VALUES(
                    'child-usage-{index}','{thread_id}','agent-{index}','child-{index}',
                    '2026-07-01T02:{index:02}:10.000000000Z',2,'gpt-5.5',
                    100,50,10,2,110,1
                 );"
            ))
            .unwrap();
    }
}

mod attribution_scale;
mod day_queries;
mod pagination_previews;
