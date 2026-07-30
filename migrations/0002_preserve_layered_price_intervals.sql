DROP VIEW priced_usage;
DROP VIEW resolved_model_prices;

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
            PARTITION BY p.model_id, p.effective_from, p.effective_to
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
