PRAGMA foreign_keys = ON;

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE app_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE source_files (
    rollout_id TEXT PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0, 1)),
    size_bytes INTEGER NOT NULL DEFAULT 0 CHECK(size_bytes >= 0),
    modified_ns INTEGER NOT NULL DEFAULT 0,
    content_fingerprint TEXT NOT NULL DEFAULT '',
    byte_offset INTEGER NOT NULL DEFAULT 0 CHECK(byte_offset >= 0),
    line_number INTEGER NOT NULL DEFAULT 0 CHECK(line_number >= 0),
    root_thread_id TEXT,
    parent_rollout_id TEXT,
    native_started INTEGER NOT NULL DEFAULT 0 CHECK(native_started IN (0, 1)),
    inherited_lines INTEGER NOT NULL DEFAULT 0 CHECK(inherited_lines >= 0),
    parse_state_json TEXT NOT NULL DEFAULT '{}',
    error_count INTEGER NOT NULL DEFAULT 0 CHECK(error_count >= 0),
    last_error TEXT,
    ingested_at TEXT NOT NULL,
    ctime_ns INTEGER,
    device_id INTEGER,
    inode INTEGER
);

CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    title TEXT,
    cwd TEXT,
    project TEXT,
    repository_url TEXT,
    branch TEXT,
    source TEXT,
    thread_source TEXT,
    source_json TEXT,
    started_at TEXT NOT NULL,
    last_event_at TEXT NOT NULL,
    title_updated_at TEXT,
    root_metadata_seen INTEGER NOT NULL DEFAULT 0 CHECK(root_metadata_seen IN (0, 1))
);

CREATE TABLE rollouts (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    parent_rollout_id TEXT,
    parent_thread_id TEXT,
    agent_path TEXT,
    agent_nickname TEXT,
    cwd TEXT,
    started_at TEXT NOT NULL,
    last_event_at TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0, 1))
);

CREATE TABLE agent_runs (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    rollout_id TEXT REFERENCES rollouts(id) ON DELETE CASCADE,
    parent_rollout_id TEXT,
    agent_path TEXT,
    nickname TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'running'
);

CREATE TABLE turns (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    rollout_id TEXT NOT NULL REFERENCES rollouts(id) ON DELETE CASCADE,
    agent_run_id TEXT REFERENCES agent_runs(id) ON DELETE SET NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    model TEXT,
    effort TEXT,
    last_agent_message TEXT,
    duration_ms INTEGER CHECK(duration_ms IS NULL OR duration_ms >= 0),
    time_to_first_token_ms INTEGER CHECK(time_to_first_token_ms IS NULL OR time_to_first_token_ms >= 0)
);

CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    rollout_id TEXT NOT NULL REFERENCES rollouts(id) ON DELETE CASCADE,
    turn_id TEXT REFERENCES turns(id) ON DELETE SET NULL,
    timestamp TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    source_line INTEGER NOT NULL CHECK(source_line >= 0)
);

CREATE TABLE events (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    rollout_id TEXT NOT NULL REFERENCES rollouts(id) ON DELETE CASCADE,
    turn_id TEXT REFERENCES turns(id) ON DELETE SET NULL,
    agent_run_id TEXT REFERENCES agent_runs(id) ON DELETE SET NULL,
    timestamp TEXT NOT NULL,
    source_line INTEGER NOT NULL CHECK(source_line >= 0),
    kind TEXT NOT NULL,
    role TEXT,
    label TEXT,
    body TEXT,
    status TEXT,
    tool_name TEXT,
    call_id TEXT,
    duration_ms INTEGER CHECK(duration_ms IS NULL OR duration_ms >= 0),
    model TEXT,
    effort TEXT,
    payload_json TEXT,
    native INTEGER NOT NULL DEFAULT 1 CHECK(native IN (0, 1))
);

CREATE TABLE tool_calls (
    id TEXT PRIMARY KEY,
    call_id TEXT NOT NULL,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    rollout_id TEXT NOT NULL REFERENCES rollouts(id) ON DELETE CASCADE,
    turn_id TEXT REFERENCES turns(id) ON DELETE SET NULL,
    agent_run_id TEXT REFERENCES agent_runs(id) ON DELETE SET NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    namespace TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    duration_ms INTEGER CHECK(duration_ms IS NULL OR duration_ms >= 0),
    UNIQUE(rollout_id, call_id)
);

CREATE TABLE usage_facts (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    rollout_id TEXT NOT NULL REFERENCES rollouts(id) ON DELETE CASCADE,
    turn_id TEXT REFERENCES turns(id) ON DELETE SET NULL,
    agent_run_id TEXT REFERENCES agent_runs(id) ON DELETE SET NULL,
    timestamp TEXT NOT NULL,
    source_line INTEGER NOT NULL CHECK(source_line >= 0),
    model TEXT NOT NULL,
    effort TEXT,
    input_tokens INTEGER NOT NULL CHECK(input_tokens >= 0),
    cached_input_tokens INTEGER NOT NULL CHECK(
        cached_input_tokens >= 0 AND cached_input_tokens <= input_tokens
    ),
    output_tokens INTEGER NOT NULL CHECK(output_tokens >= 0),
    reasoning_tokens INTEGER NOT NULL CHECK(reasoning_tokens >= 0),
    total_tokens INTEGER NOT NULL CHECK(total_tokens >= 0),
    native INTEGER NOT NULL DEFAULT 1 CHECK(native IN (0, 1))
);

CREATE TABLE model_prices (
    model_id TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to TEXT,
    input_microusd_per_million INTEGER NOT NULL CHECK(input_microusd_per_million >= 0),
    cached_input_microusd_per_million INTEGER CHECK(cached_input_microusd_per_million >= 0),
    output_microusd_per_million INTEGER NOT NULL CHECK(output_microusd_per_million >= 0),
    currency TEXT NOT NULL DEFAULT 'USD' CHECK(currency = 'USD'),
    source TEXT NOT NULL DEFAULT 'manual',
    PRIMARY KEY(model_id, effective_from, source)
);

CREATE TABLE model_aliases (
    observed_model_id TEXT NOT NULL,
    canonical_model_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    PRIMARY KEY(observed_model_id, source)
);

INSERT INTO model_aliases(
    observed_model_id,
    canonical_model_id,
    created_at,
    source
) VALUES(
    'codex-auto-review',
    'gpt-5.5',
    CURRENT_TIMESTAMP,
    'bundled-baseline'
);

CREATE INDEX idx_source_files_path ON source_files(path);
CREATE INDEX idx_rollouts_thread ON rollouts(thread_id);
CREATE INDEX idx_turns_thread_time ON turns(thread_id, started_at);
CREATE INDEX idx_messages_thread_time ON messages(thread_id, timestamp);
CREATE INDEX idx_messages_time_thread ON messages(timestamp, thread_id);
CREATE INDEX idx_events_thread_time ON events(thread_id, timestamp, source_line);
CREATE INDEX idx_events_time_thread ON events(timestamp, thread_id);
CREATE INDEX idx_events_turn ON events(turn_id, timestamp, source_line);
CREATE INDEX idx_tools_thread_time ON tool_calls(thread_id, started_at);
CREATE INDEX idx_usage_thread_time ON usage_facts(thread_id, timestamp);
CREATE INDEX idx_usage_time ON usage_facts(timestamp);
CREATE INDEX idx_usage_model_time ON usage_facts(model, timestamp);
CREATE INDEX idx_threads_last_event ON threads(last_event_at DESC, id DESC);
CREATE INDEX idx_threads_project_last_event
    ON threads(project, last_event_at DESC, id DESC);
CREATE INDEX idx_turns_time_thread ON turns(started_at, thread_id);
CREATE INDEX idx_tools_time_thread ON tool_calls(started_at, thread_id);
CREATE INDEX idx_agent_runs_thread ON agent_runs(thread_id);
CREATE INDEX idx_turns_agent_run ON turns(agent_run_id);
CREATE INDEX idx_tools_agent_run ON tool_calls(agent_run_id);
CREATE INDEX idx_usage_turn ON usage_facts(turn_id);
CREATE INDEX idx_usage_agent_run ON usage_facts(agent_run_id);
CREATE INDEX idx_usage_time_thread ON usage_facts(timestamp, thread_id);
CREATE INDEX idx_usage_overview_year ON usage_facts(
    timestamp,
    thread_id,
    model,
    input_tokens,
    cached_input_tokens,
    output_tokens,
    total_tokens
);
CREATE INDEX idx_prices_model_time ON model_prices(model_id, effective_from, source);
CREATE INDEX idx_aliases_canonical_model
    ON model_aliases(canonical_model_id, observed_model_id, source);

CREATE VIEW resolved_model_prices AS
SELECT
    model_id,
    effective_from,
    effective_to,
    input_microusd_per_million,
    cached_input_microusd_per_million,
    output_microusd_per_million,
    currency,
    source,
    source_priority
FROM (
    SELECT
        p.*,
        CASE p.source
            WHEN 'manual' THEN 3
            WHEN 'bundled-baseline' THEN 1
            ELSE 2
        END AS source_priority,
        ROW_NUMBER() OVER (
            PARTITION BY p.model_id, p.effective_from
            ORDER BY
                CASE p.source
                    WHEN 'manual' THEN 3
                    WHEN 'bundled-baseline' THEN 1
                    ELSE 2
                END DESC,
                p.source DESC
        ) AS source_rank
    FROM model_prices p
)
WHERE source_rank = 1;

CREATE VIEW resolved_model_aliases AS
SELECT observed_model_id, canonical_model_id, created_at, source
FROM (
    SELECT
        a.*,
        ROW_NUMBER() OVER (
            PARTITION BY a.observed_model_id
            ORDER BY
                CASE a.source
                    WHEN 'manual' THEN 3
                    WHEN 'bundled-baseline' THEN 1
                    ELSE 2
                END DESC,
                a.source DESC
        ) AS source_rank
    FROM model_aliases a
)
WHERE source_rank = 1;

CREATE VIEW priced_usage AS
WITH exact_usage AS (
    SELECT
        u.*,
        COALESCE(a.canonical_model_id, u.model) AS priced_model,
        p.model_id AS matched_price_model,
        CASE WHEN p.model_id IS NULL THEN 0 ELSE
            (u.input_tokens - MIN(u.input_tokens, u.cached_input_tokens))
                * p.input_microusd_per_million
            + MIN(u.input_tokens, u.cached_input_tokens)
                * COALESCE(
                    p.cached_input_microusd_per_million,
                    p.input_microusd_per_million
                )
            + u.output_tokens * p.output_microusd_per_million
        END AS cost_numerator
    FROM usage_facts u
    LEFT JOIN resolved_model_aliases a ON a.observed_model_id = u.model
    LEFT JOIN resolved_model_prices p
        ON p.model_id = COALESCE(a.canonical_model_id, u.model)
        AND (p.effective_from, p.source) = (
            SELECT p2.effective_from, p2.source
            FROM resolved_model_prices p2
            WHERE p2.model_id = COALESCE(a.canonical_model_id, u.model)
              AND p2.effective_from <= u.timestamp
              AND (p2.effective_to IS NULL OR p2.effective_to > u.timestamp)
            ORDER BY p2.source_priority DESC, p2.effective_from DESC, p2.source DESC
            LIMIT 1
        )
)
SELECT
    id,
    thread_id,
    rollout_id,
    turn_id,
    agent_run_id,
    timestamp,
    source_line,
    model,
    effort,
    input_tokens,
    cached_input_tokens,
    output_tokens,
    reasoning_tokens,
    total_tokens,
    native,
    priced_model,
    CASE WHEN matched_price_model IS NULL THEN 0 ELSE 1 END AS price_known,
    CASE WHEN matched_price_model IS NULL THEN 0
         ELSE (cost_numerator + 500000) / 1000000
    END AS cost_microusd,
    CASE WHEN matched_price_model IS NULL THEN 0.0
         ELSE CAST(cost_numerator AS REAL) / 1000000000000.0
    END AS cost_usd
FROM exact_usage;
