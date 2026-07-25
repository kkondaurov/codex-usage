use super::{UsageAccumulator, UsageTotals};
use crate::costing::{PriceBook, PriceInterval};
use anyhow::Result;
use rusqlite::{Connection, params_from_iter};
use std::collections::HashMap;

pub(crate) fn load_price_book_on(connection: &Connection) -> Result<PriceBook> {
    let mut alias_statement = connection
        .prepare("SELECT observed_model_id,canonical_model_id FROM resolved_model_aliases")?;
    let aliases = alias_statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<HashMap<_, _>, _>>()?;

    let mut price_statement = connection.prepare(
        "SELECT model_id,effective_from,effective_to,
                input_microusd_per_million,cached_input_microusd_per_million,
                output_microusd_per_million
         FROM resolved_model_prices
         ORDER BY model_id,source_priority,effective_from,source",
    )?;
    let mut prices = HashMap::<String, Vec<PriceInterval>>::new();
    let mut rows = price_statement.query([])?;
    while let Some(row) = rows.next()? {
        prices
            .entry(row.get::<_, String>(0)?)
            .or_default()
            .push(PriceInterval::new(
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ));
    }
    Ok(PriceBook::new(aliases, prices))
}

pub(super) fn read_range_totals_on(
    connection: &Connection,
    start_at: Option<&str>,
    end_at: Option<&str>,
    thread_id: Option<&str>,
) -> Result<UsageTotals> {
    let mut sql = String::from(
        "SELECT timestamp,model,input_tokens,cached_input_tokens,output_tokens,
                reasoning_tokens,total_tokens FROM usage_facts",
    );
    let mut predicates = Vec::new();
    let mut values = Vec::new();
    if let Some(start_at) = start_at {
        values.push(start_at);
        predicates.push(format!("timestamp>=?{}", values.len()));
    }
    if let Some(end_at) = end_at {
        values.push(end_at);
        predicates.push(format!("timestamp<?{}", values.len()));
    }
    if let Some(thread_id) = thread_id {
        values.push(thread_id);
        predicates.push(format!("thread_id=?{}", values.len()));
    }
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    let price_book = load_price_book_on(connection)?;
    let mut statement = connection.prepare(&sql)?;
    let mut rows = statement.query(params_from_iter(values))?;
    let mut totals = UsageAccumulator::default();
    while let Some(row) = rows.next()? {
        let timestamp = row.get::<_, String>(0)?;
        let model = row.get::<_, String>(1)?;
        totals.add_fact(
            &price_book,
            &timestamp,
            &model,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
        );
    }
    Ok(totals.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    const START: &str = "2026-01-01T00:00:00.000000000Z";
    const ONE: &str = "2026-01-01T01:00:00.000000000Z";
    const TWO: &str = "2026-01-01T02:00:00.000000000Z";

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE resolved_model_aliases(
                    observed_model_id TEXT NOT NULL,
                    canonical_model_id TEXT NOT NULL
                 );
                 CREATE TABLE resolved_model_prices(
                    model_id TEXT NOT NULL,
                    effective_from TEXT NOT NULL,
                    effective_to TEXT,
                    input_microusd_per_million INTEGER NOT NULL,
                    cached_input_microusd_per_million INTEGER,
                    output_microusd_per_million INTEGER NOT NULL,
                    source_priority INTEGER NOT NULL,
                    source TEXT NOT NULL
                 );
                 CREATE TABLE usage_facts(
                    thread_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    model TEXT NOT NULL,
                    input_tokens INTEGER NOT NULL,
                    cached_input_tokens INTEGER NOT NULL,
                    output_tokens INTEGER NOT NULL,
                    reasoning_tokens INTEGER NOT NULL,
                    total_tokens INTEGER NOT NULL
                 );",
            )
            .unwrap();
        connection
    }

    #[test]
    fn loader_preserves_aliases_and_resolved_price_order() {
        let connection = connection();
        connection
            .execute_batch(
                "INSERT INTO resolved_model_aliases VALUES('observed','canonical');
                 INSERT INTO resolved_model_prices VALUES
                    ('canonical','2026-01-01T00:00:00.000000000Z',NULL,
                     3,NULL,7,10,'bundled'),
                    ('canonical','2026-01-01T00:00:00.000000000Z',NULL,
                     11,5,13,30,'manual');",
            )
            .unwrap();

        let book = load_price_book_on(&connection).unwrap();
        let (index, price) = book.price_at("observed", START).unwrap();

        assert_eq!(index, 1);
        assert_eq!(price.cost_numerator(2, 3, 4), 89);
    }

    #[test]
    fn range_reader_is_half_open_thread_scoped_and_suppresses_partial_cost() {
        let connection = connection();
        connection
            .execute_batch(
                "INSERT INTO resolved_model_prices VALUES
                    ('priced','2026-01-01T00:00:00.000000000Z',NULL,
                     3,1,7,10,'bundled');
                 INSERT INTO usage_facts VALUES
                    ('selected','2025-12-31T23:59:59.999999999Z','priced',1,0,0,0,1),
                    ('selected','2026-01-01T00:00:00.000000000Z','priced',2,1,1,0,3),
                    ('selected','2026-01-01T01:00:00.000000000Z','unknown',4,0,1,0,5),
                    ('selected','2026-01-01T02:00:00.000000000Z','priced',9,0,0,0,9),
                    ('other','2026-01-01T00:30:00.000000000Z','priced',8,0,0,0,8);",
            )
            .unwrap();

        let totals =
            read_range_totals_on(&connection, Some(START), Some(TWO), Some("selected")).unwrap();

        assert_eq!(totals.input_tokens, 6);
        assert_eq!(totals.cached_input_tokens, 1);
        assert_eq!(totals.output_tokens, 2);
        assert_eq!(totals.total_tokens, 8);
        assert_eq!(totals.known_cost_numerator, 11);
        assert_eq!(totals.unpriced_tokens, 5);
        assert!(!totals.pricing_complete);
        assert_eq!(totals.cost_usd, None);

        let first_hour = read_range_totals_on(&connection, Some(START), Some(ONE), None).unwrap();
        assert_eq!(first_hour.total_tokens, 11);
        assert_eq!(first_hour.known_cost_numerator, 35);
        assert_eq!(first_hour.unpriced_tokens, 0);
    }
}
