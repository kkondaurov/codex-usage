use super::MAX_MODEL_ID_CHARS;
use crate::costing::PriceMicros;
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use unicode_normalization::UnicodeNormalization;

pub(super) const MAX_SEARCH_CHARS: usize = 256;
pub(super) const MAX_MODEL_ID_RESULTS: u64 = 100;
pub(super) const MAX_UNKNOWN_MODEL_RESULTS: u64 = 100;

#[derive(Debug)]
pub(super) struct PriceRecord {
    pub(super) model_id: String,
    pub(super) effective_from: String,
    pub(super) effective_to: Option<String>,
    pub(super) input_per_million: PriceMicros,
    pub(super) cached_input_per_million: Option<PriceMicros>,
    pub(super) output_per_million: PriceMicros,
    pub(super) currency: String,
    pub(super) source: String,
}

#[derive(Debug)]
pub(super) struct PriceListing {
    pub(super) items: Vec<PriceRecord>,
    pub(super) page: u64,
    pub(super) page_size: u64,
    pub(super) total: u64,
    pub(super) total_pages: u64,
    pub(super) last_refresh_at: Option<String>,
    pub(super) last_refresh_error_at: Option<String>,
    pub(super) refresh_error_kind: Option<String>,
    pub(super) refresh_error: Option<String>,
    pub(super) source: Option<String>,
}

#[derive(Debug)]
pub(super) struct AliasRecord {
    pub(super) observed_model_id: String,
    pub(super) canonical_model_id: String,
}

#[derive(Debug)]
pub(super) struct AliasListing {
    pub(super) items: Vec<AliasRecord>,
    pub(super) page: u64,
    pub(super) page_size: u64,
    pub(super) total: u64,
    pub(super) total_pages: u64,
}

#[derive(Debug)]
pub(super) struct UnknownModel {
    pub(super) model_id: String,
    pub(super) usage_count: u64,
    pub(super) total_tokens: u64,
    pub(super) last_seen_at: String,
}

#[derive(Debug)]
pub(super) struct PriceMetadata {
    pub(super) observed_unknown: Vec<UnknownModel>,
    pub(super) observed_unknown_total: u64,
}

pub(super) fn prices(
    connection: &Connection,
    q: Option<&str>,
    page: u64,
    page_size: u64,
) -> Result<PriceListing> {
    anyhow::ensure!((1..=100).contains(&page_size), "invalid price page size");
    anyhow::ensure!(page > 0, "invalid price page");
    let q_filter = q.filter(|value| !value.trim().is_empty());
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS price_search_matches(
             model_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         DELETE FROM price_search_matches;",
    )?;
    if let Some(query) = q_filter {
        let needle = normalize_search_text(query.trim());
        let mut select = connection.prepare("SELECT model_id FROM resolved_model_prices")?;
        let mut insert = connection
            .prepare("INSERT OR IGNORE INTO price_search_matches(model_id) VALUES(?1)")?;
        let mut rows = select.query([])?;
        while let Some(row) = rows.next()? {
            let model_id = row.get::<_, String>(0)?;
            if normalize_search_text(&model_id).contains(&needle) {
                insert.execute([&model_id])?;
            }
        }
    }
    let total: i64 = connection.query_row(
        "SELECT COUNT(*) FROM resolved_model_prices
         WHERE ?1 IS NULL OR EXISTS(
             SELECT 1 FROM price_search_matches search
             WHERE search.model_id=resolved_model_prices.model_id
         )",
        [q_filter],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT model_id,effective_from,effective_to,input_microusd_per_million,
                cached_input_microusd_per_million,output_microusd_per_million,currency,source
         FROM resolved_model_prices
         WHERE ?1 IS NULL OR EXISTS(
             SELECT 1 FROM price_search_matches search
             WHERE search.model_id=resolved_model_prices.model_id
         )
         ORDER BY model_id,effective_from DESC,
                  effective_to IS NULL DESC,effective_to DESC,
                  source_priority DESC,source DESC
         LIMIT ?2 OFFSET ?3",
    )?;
    let raw_items = statement
        .query_map(
            params![q_filter, page_size as i64, page_offset(page, page_size)],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let items = raw_items
        .into_iter()
        .map(
            |(model_id, effective_from, effective_to, input, cached, output, currency, source)| {
                Ok(PriceRecord {
                    model_id,
                    effective_from,
                    effective_to,
                    input_per_million: PriceMicros::from_raw(input)?,
                    cached_input_per_million: cached.map(PriceMicros::from_raw).transpose()?,
                    output_per_million: PriceMicros::from_raw(output)?,
                    currency,
                    source,
                })
            },
        )
        .collect::<Result<Vec<_>>>()?;
    let total = total.max(0) as u64;
    let last_refresh_at = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_last_refresh_at'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let last_refresh_error_at = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_last_error_at'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let refresh_error = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_last_error'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|value| value.chars().take(512).collect());
    let refresh_error_kind = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_last_error_kind'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let source = connection
        .query_row(
            "SELECT value FROM app_meta WHERE key='pricing_source_url'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(PriceListing {
        items,
        page,
        page_size,
        total,
        total_pages: total.div_ceil(page_size),
        last_refresh_at,
        last_refresh_error_at,
        refresh_error_kind,
        refresh_error,
        source,
    })
}

pub(super) fn aliases(
    connection: &Connection,
    q: Option<&str>,
    page: u64,
    page_size: u64,
) -> Result<AliasListing> {
    anyhow::ensure!((1..=100).contains(&page_size), "invalid alias page size");
    anyhow::ensure!(page > 0, "invalid alias page");
    let q_filter = q.map(str::trim).filter(|value| !value.is_empty());
    anyhow::ensure!(
        q_filter.is_none_or(|value| value.chars().count() <= MAX_SEARCH_CHARS),
        "alias search exceeds the {MAX_SEARCH_CHARS}-character limit"
    );
    anyhow::ensure!(
        q_filter
            .is_none_or(|value| normalize_search_text(value).chars().count() <= MAX_SEARCH_CHARS),
        "normalized alias search exceeds the {MAX_SEARCH_CHARS}-character limit"
    );
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS alias_search_matches(
             observed_model_id TEXT PRIMARY KEY
         ) WITHOUT ROWID;
         DELETE FROM alias_search_matches;",
    )?;
    if let Some(query) = q_filter {
        let needle = normalize_search_text(query);
        let mut select = connection.prepare(
            "SELECT observed_model_id,canonical_model_id
             FROM resolved_model_aliases
             WHERE length(observed_model_id) BETWEEN 1 AND ?1
               AND length(canonical_model_id) BETWEEN 1 AND ?1",
        )?;
        let mut insert = connection
            .prepare("INSERT OR IGNORE INTO alias_search_matches(observed_model_id) VALUES(?1)")?;
        let mut rows = select.query([MAX_MODEL_ID_CHARS as i64])?;
        while let Some(row) = rows.next()? {
            let observed_model_id = row.get::<_, String>(0)?;
            let canonical_model_id = row.get::<_, String>(1)?;
            if normalize_search_text(&observed_model_id).contains(&needle)
                || normalize_search_text(&canonical_model_id).contains(&needle)
            {
                insert.execute([&observed_model_id])?;
            }
        }
    }
    let total = connection.query_row(
        "SELECT COUNT(*) FROM resolved_model_aliases
         WHERE length(observed_model_id) BETWEEN 1 AND ?2
           AND length(canonical_model_id) BETWEEN 1 AND ?2
           AND (
                ?1 IS NULL OR EXISTS(
                    SELECT 1 FROM alias_search_matches search
                    WHERE search.observed_model_id=resolved_model_aliases.observed_model_id
                )
           )",
        params![q_filter, MAX_MODEL_ID_CHARS as i64],
        |row| row.get::<_, i64>(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT observed_model_id,canonical_model_id
         FROM resolved_model_aliases
         WHERE length(observed_model_id) BETWEEN 1 AND ?2
           AND length(canonical_model_id) BETWEEN 1 AND ?2
           AND (
                ?1 IS NULL OR EXISTS(
                    SELECT 1 FROM alias_search_matches search
                    WHERE search.observed_model_id=resolved_model_aliases.observed_model_id
                )
           )
         ORDER BY observed_model_id
         LIMIT ?3 OFFSET ?4",
    )?;
    let items = statement
        .query_map(
            params![
                q_filter,
                MAX_MODEL_ID_CHARS as i64,
                page_size as i64,
                page_offset(page, page_size),
            ],
            |row| {
                Ok(AliasRecord {
                    observed_model_id: row.get(0)?,
                    canonical_model_id: row.get(1)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let total = total.max(0) as u64;
    Ok(AliasListing {
        items,
        page,
        page_size,
        total,
        total_pages: total.div_ceil(page_size),
    })
}

pub(super) fn metadata(connection: &Connection, unknown_limit: u64) -> Result<PriceMetadata> {
    anyhow::ensure!(
        (1..=MAX_UNKNOWN_MODEL_RESULTS).contains(&unknown_limit),
        "invalid unknown model result limit"
    );
    let observed_unknown_total: i64 = connection.query_row(
        "SELECT COUNT(*) FROM (
            SELECT model FROM priced_usage WHERE price_known=0 GROUP BY model
         )",
        [],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT model,COUNT(*),SUM(total_tokens),MAX(timestamp) FROM priced_usage
         WHERE price_known=0 AND length(model) BETWEEN 1 AND ?1 GROUP BY model
         ORDER BY SUM(total_tokens) DESC,model LIMIT ?2",
    )?;
    let observed_unknown = statement
        .query_map(
            params![MAX_MODEL_ID_CHARS as i64, unknown_limit as i64],
            |row| {
                Ok(UnknownModel {
                    model_id: row.get(0)?,
                    usage_count: row.get::<_, i64>(1)?.max(0) as u64,
                    total_tokens: row.get::<_, i64>(2)?.max(0) as u64,
                    last_seen_at: row.get(3)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(PriceMetadata {
        observed_unknown,
        observed_unknown_total: observed_unknown_total.max(0) as u64,
    })
}

pub(super) fn model_ids(
    connection: &Connection,
    q: Option<&str>,
    limit: u64,
) -> Result<Vec<String>> {
    anyhow::ensure!(
        (1..=MAX_MODEL_ID_RESULTS).contains(&limit),
        "invalid model ID result limit"
    );
    let needle = q
        .filter(|value| !value.trim().is_empty())
        .map(|value| normalize_search_text(value.trim()));
    anyhow::ensure!(
        needle
            .as_deref()
            .is_none_or(|value| value.chars().count() <= MAX_SEARCH_CHARS),
        "model ID search exceeds the {MAX_SEARCH_CHARS}-character limit"
    );

    let mut statement = connection
        .prepare("SELECT DISTINCT model_id FROM resolved_model_prices ORDER BY model_id")?;
    let mut rows = statement.query([])?;
    let mut items = Vec::with_capacity(limit as usize);
    while let Some(row) = rows.next()? {
        let model_id = row.get::<_, String>(0)?;
        if model_id.chars().count() > MAX_MODEL_ID_CHARS {
            continue;
        }
        if needle
            .as_deref()
            .is_none_or(|needle| normalize_search_text(&model_id).contains(needle))
        {
            items.push(model_id);
            if items.len() == limit as usize {
                break;
            }
        }
    }
    Ok(items)
}

pub(super) fn normalize_search_text(value: &str) -> String {
    value
        .nfkc()
        .flat_map(char::to_lowercase)
        .collect::<String>()
}

fn page_offset(page: u64, page_size: u64) -> i64 {
    page.saturating_sub(1)
        .saturating_mul(page_size)
        .min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Db;

    #[test]
    fn page_offset_saturates_before_sql_conversion() {
        assert_eq!(page_offset(1, 25), 0);
        assert_eq!(page_offset(3, 25), 50);
        assert_eq!(page_offset(u64::MAX, 100), i64::MAX);
    }

    #[test]
    fn catalog_pagination_enforces_internal_shapes_without_a_transport_ceiling() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();

        assert!(prices(&connection, None, u64::MAX, 1).is_ok());
        assert!(aliases(&connection, None, u64::MAX, 1).is_ok());

        for error in [
            prices(&connection, None, 0, 1).unwrap_err(),
            prices(&connection, None, 1, 0).unwrap_err(),
            prices(&connection, None, 1, 101).unwrap_err(),
            aliases(&connection, None, 0, 1).unwrap_err(),
            aliases(&connection, None, 1, 0).unwrap_err(),
            aliases(&connection, None, 1, 101).unwrap_err(),
        ] {
            assert!(error.to_string().starts_with("invalid "));
        }
    }

    #[test]
    fn price_history_pagination_is_stable_for_layers_with_the_same_start() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO model_prices(
                    model_id,effective_from,effective_to,
                    input_microusd_per_million,cached_input_microusd_per_million,
                    output_microusd_per_million,currency,source
                 ) VALUES(
                    'gpt-5.5','1970-01-01T00:00:00.000000000Z',
                    '2026-07-30T13:00:00.000000000Z',
                    9000000,900000,40000000,'USD','remote:legacy'
                 )",
                [],
            )
            .unwrap();

        let current = prices(&connection, Some("gpt-5.5"), 1, 1).unwrap();
        let historical = prices(&connection, Some("gpt-5.5"), 2, 1).unwrap();
        assert_eq!(current.total, 2);
        assert_eq!(current.total_pages, 2);
        assert_eq!(current.items[0].source, "bundled-baseline");
        assert_eq!(current.items[0].effective_to, None);
        assert_eq!(historical.items[0].source, "remote:legacy");
        assert_eq!(
            historical.items[0].effective_to.as_deref(),
            Some("2026-07-30T13:00:00.000000000Z")
        );
    }

    #[test]
    fn aliases_are_filtered_and_paginated_before_serialization() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<51
                 )
                 INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 )
                 SELECT printf('legacy-alias-%02d',value),
                        printf('canonical-target-%02d',value),
                        '2026-01-01T00:00:00.000000000Z','remote:test'
                 FROM sequence;
                 INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 ) VALUES(
                    replace(hex(zeroblob(300)),'00','x'),'canonical-target-00',
                    '2026-01-01T00:00:00.000000000Z','remote:test'
                 );",
            )
            .unwrap();

        let page = aliases(&connection, Some("LEGACY-ALIAS-"), 2, 10).unwrap();
        assert_eq!(page.page, 2);
        assert_eq!(page.page_size, 10);
        assert_eq!(page.total, 52);
        assert_eq!(page.total_pages, 6);
        assert_eq!(page.items.len(), 10);
        assert_eq!(page.items[0].observed_model_id, "legacy-alias-10");
        assert!(page.items.iter().all(|alias| {
            alias.observed_model_id.chars().count() <= MAX_MODEL_ID_CHARS
                && alias.canonical_model_id.chars().count() <= MAX_MODEL_ID_CHARS
        }));

        let canonical = aliases(&connection, Some("TARGET-3"), 1, 25).unwrap();
        assert_eq!(canonical.total, 10);
        assert_eq!(canonical.items.len(), 10);
    }

    #[test]
    fn alias_search_normalizes_unicode_before_deterministic_pagination() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO model_aliases(
                    observed_model_id,canonical_model_id,created_at,source
                 ) VALUES
                    ('MÜNCHEN-É-02','gpt-5.5','2026-01-01T00:00:00Z','remote:test'),
                    ('MÜNCHEN-É-01','gpt-5.5','2026-01-01T00:00:00Z','remote:test');",
            )
            .unwrap();

        let first = aliases(&connection, Some("münchen-e\u{301}"), 1, 1).unwrap();
        assert_eq!(first.total, 2);
        assert_eq!(first.total_pages, 2);
        assert_eq!(first.items[0].observed_model_id, "MÜNCHEN-É-01");

        let second = aliases(&connection, Some("münchen-e\u{301}"), 2, 1).unwrap();
        assert_eq!(second.total, 2);
        assert_eq!(second.items[0].observed_model_id, "MÜNCHEN-É-02");
    }

    #[test]
    fn metadata_bounds_twenty_thousand_unknown_models() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "INSERT INTO threads(id,title,started_at,last_event_at)
                 VALUES('unknowns','Unknowns','2026-01-01T00:00:00.000000000Z',
                        '2026-01-01T00:00:00.000000000Z');
                 INSERT INTO rollouts(id,thread_id,started_at,last_event_at,archived)
                 VALUES('unknowns','unknowns','2026-01-01T00:00:00.000000000Z',
                        '2026-01-01T00:00:00.000000000Z',0);
                 WITH RECURSIVE sequence(value) AS (
                    SELECT 0 UNION ALL SELECT value+1 FROM sequence WHERE value<19999
                 )
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 )
                 SELECT printf('unknown-fact-%05d',value),'unknowns','unknowns',
                        '2026-01-01T00:00:00.000000000Z',value+1,
                        printf('unknown-model-%05d',value),1,0,0,0,1,1
                 FROM sequence;
                 INSERT INTO usage_facts(
                    id,thread_id,rollout_id,timestamp,source_line,model,
                    input_tokens,cached_input_tokens,output_tokens,
                    reasoning_tokens,total_tokens,native
                 ) VALUES(
                    'unknown-fact-overlong','unknowns','unknowns',
                    '2026-01-01T00:00:00.000000000Z',20001,
                    replace(hex(zeroblob(300)),'00','y'),1000000,0,0,0,1000000,1
                 );",
            )
            .unwrap();

        let metadata = metadata(&connection, 100).unwrap();
        assert_eq!(metadata.observed_unknown_total, 20_001);
        assert_eq!(metadata.observed_unknown.len(), 100);
        assert!(
            metadata
                .observed_unknown
                .iter()
                .all(|row| row.model_id.chars().count() <= MAX_MODEL_ID_CHARS)
        );
    }

    #[test]
    fn model_ids_filter_order_limit_and_reject_overlong_rows() {
        let temp = tempfile::tempdir().unwrap();
        let db = Db::open(temp.path().join("usage.db")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute_batch(
                "DELETE FROM model_prices;
                 INSERT INTO model_prices(
                    model_id,effective_from,input_microusd_per_million,
                    cached_input_microusd_per_million,output_microusd_per_million,
                    currency,source
                 ) VALUES
                    ('alpha-model','1970-01-01T00:00:00Z',1,1,1,'USD','manual'),
                    ('beta-model','1970-01-01T00:00:00Z',1,1,1,'USD','manual'),
                    ('zeta-model','1970-01-01T00:00:00Z',1,1,1,'USD','manual'),
                    ('Éclair-model','1970-01-01T00:00:00Z',1,1,1,'USD','manual'),
                    (replace(hex(zeroblob(300)),'00','x'),
                     '1970-01-01T00:00:00Z',1,1,1,'USD','manual');",
            )
            .unwrap();

        assert_eq!(
            model_ids(&connection, None, 2).unwrap(),
            ["alpha-model", "beta-model"]
        );
        assert_eq!(
            model_ids(&connection, Some("e\u{301}clair"), 100).unwrap(),
            ["Éclair-model"]
        );
        let all = model_ids(&connection, None, 100).unwrap();
        assert_eq!(all.len(), 4);
        assert!(
            all.iter()
                .all(|model_id| model_id.chars().count() <= MAX_MODEL_ID_CHARS)
        );
    }
}
