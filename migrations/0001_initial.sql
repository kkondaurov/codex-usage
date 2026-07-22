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

-- Activity pages render a filtered, lifecycle-deduplicated view of events.
-- Persist the small identity/sort projection once during ingestion so every
-- child page does not have to group and sort the complete turn history.
-- Events and messages are append-only after insertion. Ingestion may replace
-- a complete rollout transactionally: deleting its events cascades into this
-- table, and reinserting the rollout rebuilds the projection through the
-- insert triggers below. Surgical event/message updates or deletes are not a
-- supported write path.
CREATE TABLE activity_event_index (
    event_id TEXT PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
    thread_id TEXT NOT NULL,
    turn_key TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    source_line INTEGER NOT NULL CHECK(source_line >= 0),
    canonical_key TEXT NOT NULL,
    UNIQUE(thread_id, turn_key, canonical_key)
);

CREATE TRIGGER project_activity_event_after_insert
AFTER INSERT ON events
WHEN NEW.kind NOT IN ('turn_started', 'system', 'tool_output', 'tool_completed')
BEGIN
    INSERT INTO activity_event_index(
        event_id,thread_id,turn_key,timestamp,source_line,canonical_key
    )
    SELECT
        NEW.id,
        NEW.thread_id,
        COALESCE(NEW.turn_id,''),
        NEW.timestamp,
        NEW.source_line,
        CASE
            WHEN NEW.kind='tool_call' AND NEW.call_id IS NOT NULL
                THEN 'tool:' || json_array(NEW.rollout_id,NEW.call_id)
            ELSE 'event:' || NEW.id
        END
    WHERE NOT (
        NEW.kind='turn_completed'
        AND EXISTS(
            SELECT 1
            FROM events final_event
            LEFT JOIN messages final_message
              ON final_message.id=COALESCE(final_event.call_id,final_event.id)
             AND final_message.thread_id=final_event.thread_id
            WHERE final_event.thread_id=NEW.thread_id
              AND final_event.turn_id IS NEW.turn_id
              AND final_event.kind='final'
              AND trim(COALESCE(final_event.body,final_message.content,''))<>''
        )
    )
    ON CONFLICT(thread_id,turn_key,canonical_key) DO UPDATE SET
        event_id=excluded.event_id,
        timestamp=excluded.timestamp,
        source_line=excluded.source_line
    WHERE excluded.source_line<activity_event_index.source_line
       OR (
            excluded.source_line=activity_event_index.source_line
            AND excluded.event_id<activity_event_index.event_id
       );

    -- A final response supersedes a generic task-complete marker. Usually the
    -- final arrives first, but this makes the projection correct for either
    -- source order.
    DELETE FROM activity_event_index
    WHERE NEW.kind='final'
      AND trim(COALESCE(
            NEW.body,
            (SELECT content FROM messages
             WHERE id=COALESCE(NEW.call_id,NEW.id)
               AND thread_id=NEW.thread_id),
            ''
          ))<>''
      AND event_id IN (
            SELECT completed.id
            FROM events completed
            WHERE completed.thread_id=NEW.thread_id
              AND completed.turn_id IS NEW.turn_id
              AND completed.kind='turn_completed'
      );
END;

-- Some source formats insert the message row after its event envelope. Apply
-- the same final-vs-completed rule when that body becomes available.
CREATE TRIGGER hide_activity_completed_after_message_insert
AFTER INSERT ON messages
BEGIN
    DELETE FROM activity_event_index
    WHERE event_id IN (
        SELECT completed.id
        FROM events final_event
        JOIN events completed
          ON completed.thread_id=final_event.thread_id
         AND completed.turn_id IS final_event.turn_id
         AND completed.kind='turn_completed'
        WHERE final_event.thread_id=NEW.thread_id
          AND final_event.kind='final'
          AND COALESCE(final_event.call_id,final_event.id)=NEW.id
          AND trim(NEW.content)<>''
    );
END;

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
    -- These explicit domains keep the largest possible price multiplication
    -- below i64::MAX, so SQLite cannot silently promote fixed-point arithmetic
    -- to REAL. The application enforces the same limits at its boundaries.
    input_tokens INTEGER NOT NULL CHECK(
        typeof(input_tokens) = 'integer' AND input_tokens BETWEEN 0 AND 4000000000
    ),
    cached_input_tokens INTEGER NOT NULL CHECK(
        typeof(cached_input_tokens) = 'integer'
        AND cached_input_tokens BETWEEN 0 AND 4000000000
        AND cached_input_tokens <= input_tokens
    ),
    output_tokens INTEGER NOT NULL CHECK(
        typeof(output_tokens) = 'integer' AND output_tokens BETWEEN 0 AND 4000000000
    ),
    reasoning_tokens INTEGER NOT NULL CHECK(
        typeof(reasoning_tokens) = 'integer'
        AND reasoning_tokens BETWEEN 0 AND 4000000000
        AND reasoning_tokens <= output_tokens
    ),
    total_tokens INTEGER NOT NULL CHECK(
        typeof(total_tokens) = 'integer'
        AND total_tokens BETWEEN 0 AND 4000000000
        AND total_tokens = input_tokens + output_tokens
    ),
    native INTEGER NOT NULL DEFAULT 1 CHECK(native IN (0, 1))
);

-- This one-row guard bounds the sum of every nonnegative usage column. Besides
-- protecting its own trigger arithmetic, it proves that SUM(...) over any
-- subset of usage facts or rollups remains a signed SQLite integer.
CREATE TABLE usage_global_totals (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    fact_count INTEGER NOT NULL CHECK(
        typeof(fact_count) = 'integer'
        AND fact_count BETWEEN 0 AND 9007199254740991
    ),
    input_tokens INTEGER NOT NULL CHECK(
        typeof(input_tokens) = 'integer'
        AND input_tokens BETWEEN 0 AND 9007199254740991
    ),
    cached_input_tokens INTEGER NOT NULL CHECK(
        typeof(cached_input_tokens) = 'integer'
        AND cached_input_tokens BETWEEN 0 AND 9007199254740991
        AND cached_input_tokens <= input_tokens
    ),
    output_tokens INTEGER NOT NULL CHECK(
        typeof(output_tokens) = 'integer'
        AND output_tokens BETWEEN 0 AND 9007199254740991
    ),
    reasoning_tokens INTEGER NOT NULL CHECK(
        typeof(reasoning_tokens) = 'integer'
        AND reasoning_tokens BETWEEN 0 AND 9007199254740991
        AND reasoning_tokens <= output_tokens
    ),
    total_tokens INTEGER NOT NULL CHECK(
        typeof(total_tokens) = 'integer'
        AND total_tokens BETWEEN 0 AND 9007199254740991
        AND total_tokens = input_tokens + output_tokens
    )
);

INSERT INTO usage_global_totals(
    id,fact_count,input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
) VALUES(1,0,0,0,0,0,0);

CREATE TRIGGER usage_global_totals_insert
AFTER INSERT ON usage_facts
BEGIN
    UPDATE usage_global_totals SET
        fact_count=fact_count+1,
        input_tokens=input_tokens+NEW.input_tokens,
        cached_input_tokens=cached_input_tokens+NEW.cached_input_tokens,
        output_tokens=output_tokens+NEW.output_tokens,
        reasoning_tokens=reasoning_tokens+NEW.reasoning_tokens,
        total_tokens=total_tokens+NEW.total_tokens
    WHERE id=1;
END;

CREATE TRIGGER usage_global_totals_delete
AFTER DELETE ON usage_facts
BEGIN
    UPDATE usage_global_totals SET
        fact_count=fact_count-1,
        input_tokens=input_tokens-OLD.input_tokens,
        cached_input_tokens=cached_input_tokens-OLD.cached_input_tokens,
        output_tokens=output_tokens-OLD.output_tokens,
        reasoning_tokens=reasoning_tokens-OLD.reasoning_tokens,
        total_tokens=total_tokens-OLD.total_tokens
    WHERE id=1;
END;

CREATE TRIGGER usage_global_totals_update
AFTER UPDATE OF input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens ON usage_facts
BEGIN
    UPDATE usage_global_totals SET
        input_tokens=input_tokens-OLD.input_tokens+NEW.input_tokens,
        cached_input_tokens=cached_input_tokens-OLD.cached_input_tokens+NEW.cached_input_tokens,
        output_tokens=output_tokens-OLD.output_tokens+NEW.output_tokens,
        reasoning_tokens=reasoning_tokens-OLD.reasoning_tokens+NEW.reasoning_tokens,
        total_tokens=total_tokens-OLD.total_tokens+NEW.total_tokens
    WHERE id=1;
END;

-- Raw usage remains the source of truth. This compact projection keeps the token
-- shape needed for query-time repricing while making Activity and cost-sorted
-- session queries proportional to active turn-hours rather than usage-fact count.
-- UTC buckets remain stable if the machine changes time zone; Activity derives
-- local calendar days from them at read time.
CREATE TABLE usage_activity_rollups (
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    rollout_id TEXT NOT NULL,
    turn_key TEXT NOT NULL,
    activity_hour TEXT NOT NULL,
    model TEXT NOT NULL,
    fact_count INTEGER NOT NULL CHECK(
        typeof(fact_count) = 'integer'
        AND fact_count BETWEEN 1 AND 9007199254740991
    ),
    input_tokens INTEGER NOT NULL CHECK(
        typeof(input_tokens) = 'integer'
        AND input_tokens BETWEEN 0 AND 9007199254740991
    ),
    cached_input_tokens INTEGER NOT NULL CHECK(
        typeof(cached_input_tokens) = 'integer'
        AND cached_input_tokens BETWEEN 0 AND 9007199254740991
        AND cached_input_tokens <= input_tokens
    ),
    output_tokens INTEGER NOT NULL CHECK(
        typeof(output_tokens) = 'integer'
        AND output_tokens BETWEEN 0 AND 9007199254740991
    ),
    reasoning_tokens INTEGER NOT NULL CHECK(
        typeof(reasoning_tokens) = 'integer'
        AND reasoning_tokens BETWEEN 0 AND 9007199254740991
        AND reasoning_tokens <= output_tokens
    ),
    total_tokens INTEGER NOT NULL CHECK(
        typeof(total_tokens) = 'integer'
        AND total_tokens BETWEEN 0 AND 9007199254740991
        AND total_tokens = input_tokens + output_tokens
    ),
    PRIMARY KEY(thread_id, rollout_id, turn_key, activity_hour, model)
) WITHOUT ROWID;

CREATE TRIGGER usage_activity_rollups_insert
AFTER INSERT ON usage_facts
WHEN strftime('%Y-%m-%dT%H:00:00.000000000Z',NEW.timestamp) IS NOT NULL
BEGIN
    INSERT INTO usage_activity_rollups(
        thread_id,rollout_id,turn_key,activity_hour,model,fact_count,
        input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
    ) VALUES(
        NEW.thread_id,NEW.rollout_id,COALESCE(NEW.turn_id,''),
        strftime('%Y-%m-%dT%H:00:00.000000000Z',NEW.timestamp),NEW.model,1,
        NEW.input_tokens,NEW.cached_input_tokens,NEW.output_tokens,
        NEW.reasoning_tokens,NEW.total_tokens
    )
    ON CONFLICT(thread_id,rollout_id,turn_key,activity_hour,model) DO UPDATE SET
        fact_count=fact_count+1,
        input_tokens=input_tokens+excluded.input_tokens,
        cached_input_tokens=cached_input_tokens+excluded.cached_input_tokens,
        output_tokens=output_tokens+excluded.output_tokens,
        reasoning_tokens=reasoning_tokens+excluded.reasoning_tokens,
        total_tokens=total_tokens+excluded.total_tokens;
END;

CREATE TRIGGER usage_activity_rollups_delete
AFTER DELETE ON usage_facts
WHEN strftime('%Y-%m-%dT%H:00:00.000000000Z',OLD.timestamp) IS NOT NULL
BEGIN
    DELETE FROM usage_activity_rollups
    WHERE thread_id=OLD.thread_id AND rollout_id=OLD.rollout_id
      AND turn_key=COALESCE(OLD.turn_id,'')
      AND activity_hour=strftime('%Y-%m-%dT%H:00:00.000000000Z',OLD.timestamp)
      AND model=OLD.model
      AND fact_count=1;
    UPDATE usage_activity_rollups SET
        fact_count=fact_count-1,
        input_tokens=input_tokens-OLD.input_tokens,
        cached_input_tokens=cached_input_tokens-OLD.cached_input_tokens,
        output_tokens=output_tokens-OLD.output_tokens,
        reasoning_tokens=reasoning_tokens-OLD.reasoning_tokens,
        total_tokens=total_tokens-OLD.total_tokens
    WHERE thread_id=OLD.thread_id AND rollout_id=OLD.rollout_id
      AND turn_key=COALESCE(OLD.turn_id,'')
      AND activity_hour=strftime('%Y-%m-%dT%H:00:00.000000000Z',OLD.timestamp)
      AND model=OLD.model;
END;

-- Foreign-key actions can update turn_id in place (ON DELETE SET NULL), and
-- maintenance code may correct a fact without deleting it first. Keep the
-- derived rollup exact for every UPDATE, not only the ingest delete/insert path.
CREATE TRIGGER usage_activity_rollups_update
AFTER UPDATE OF thread_id,rollout_id,turn_id,timestamp,model,
                input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens ON usage_facts
BEGIN
    DELETE FROM usage_activity_rollups
    WHERE thread_id=OLD.thread_id AND rollout_id=OLD.rollout_id
      AND turn_key=COALESCE(OLD.turn_id,'')
      AND activity_hour=strftime('%Y-%m-%dT%H:00:00.000000000Z',OLD.timestamp)
      AND model=OLD.model
      AND fact_count=1;
    UPDATE usage_activity_rollups SET
        fact_count=fact_count-1,
        input_tokens=input_tokens-OLD.input_tokens,
        cached_input_tokens=cached_input_tokens-OLD.cached_input_tokens,
        output_tokens=output_tokens-OLD.output_tokens,
        reasoning_tokens=reasoning_tokens-OLD.reasoning_tokens,
        total_tokens=total_tokens-OLD.total_tokens
    WHERE thread_id=OLD.thread_id AND rollout_id=OLD.rollout_id
      AND turn_key=COALESCE(OLD.turn_id,'')
      AND activity_hour=strftime('%Y-%m-%dT%H:00:00.000000000Z',OLD.timestamp)
      AND model=OLD.model;

    INSERT INTO usage_activity_rollups(
        thread_id,rollout_id,turn_key,activity_hour,model,fact_count,
        input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,total_tokens
    )
    SELECT NEW.thread_id,NEW.rollout_id,COALESCE(NEW.turn_id,''),
           strftime('%Y-%m-%dT%H:00:00.000000000Z',NEW.timestamp),NEW.model,1,
           NEW.input_tokens,NEW.cached_input_tokens,NEW.output_tokens,
           NEW.reasoning_tokens,NEW.total_tokens
    WHERE strftime('%Y-%m-%dT%H:00:00.000000000Z',NEW.timestamp) IS NOT NULL
    ON CONFLICT(thread_id,rollout_id,turn_key,activity_hour,model) DO UPDATE SET
        fact_count=fact_count+1,
        input_tokens=input_tokens+excluded.input_tokens,
        cached_input_tokens=cached_input_tokens+excluded.cached_input_tokens,
        output_tokens=output_tokens+excluded.output_tokens,
        reasoning_tokens=reasoning_tokens+excluded.reasoning_tokens,
        total_tokens=total_tokens+excluded.total_tokens;
END;

CREATE TABLE model_prices (
    model_id TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to TEXT,
    input_microusd_per_million INTEGER NOT NULL CHECK(
        typeof(input_microusd_per_million) = 'integer'
        AND input_microusd_per_million BETWEEN 0 AND 1000000000
    ),
    cached_input_microusd_per_million INTEGER CHECK(
        cached_input_microusd_per_million IS NULL
        OR (
            typeof(cached_input_microusd_per_million) = 'integer'
            AND cached_input_microusd_per_million BETWEEN 0 AND 1000000000
        )
    ),
    output_microusd_per_million INTEGER NOT NULL CHECK(
        typeof(output_microusd_per_million) = 'integer'
        AND output_microusd_per_million BETWEEN 0 AND 1000000000
    ),
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
CREATE INDEX idx_activity_event_index_thread_time
    ON activity_event_index(thread_id, timestamp DESC, source_line DESC, event_id DESC);
CREATE INDEX idx_activity_event_index_turn_time
    ON activity_event_index(
        thread_id, turn_key, timestamp DESC, source_line DESC, event_id DESC
    );
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
CREATE INDEX idx_usage_activity_owner
    ON usage_facts(thread_id, rollout_id, turn_id, source_line);
CREATE INDEX idx_events_activity_owner
    ON events(thread_id, rollout_id, turn_id, source_line, kind);
CREATE INDEX idx_usage_thread_model_time
    ON usage_facts(thread_id, model, timestamp);
CREATE INDEX idx_usage_turn_model_time
    ON usage_facts(thread_id, turn_id, model, timestamp);
CREATE INDEX idx_usage_activity_rollups_turn
    ON usage_activity_rollups(thread_id, turn_key, activity_hour, model);
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
    cost_numerator
FROM exact_usage;
