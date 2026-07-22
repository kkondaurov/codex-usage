use anyhow::{Context, Result};
use codex_usage::{
    api::{self, ApiState},
    config::{Command, parse_cli, require_repository_root},
    db::Db,
    db_executor::DbExecutor,
    ingest::{self, IngestRoots},
    pricing::PricingSync,
};
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    restrict_process_file_creation();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = parse_cli();
    require_repository_root()?;
    match cli.command.expect("the parser always supplies a command") {
        Command::Ingest(args) => {
            let common = args.common.resolved();
            let db = open_configured_db(&common)?;
            ingest::recover_interrupted_scan(&db)?;
            PricingSync::new(DbExecutor::default())
                .sync_if_needed(&db, &common.pricing())
                .await?;
            let report = ingest::scan_once(
                &db,
                &IngestRoots {
                    active: common.sessions,
                    archive: common.archive,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Serve(args) => {
            let address = args.bind_address()?;
            args.require_frontend_build()?;
            // Reserve the public process boundary before opening SQLite or
            // starting background work. If another server already owns this
            // address, startup must fail without migrating, seeding, scanning,
            // or refreshing any local state.
            let listener = tokio::net::TcpListener::bind(address)
                .await
                .with_context(|| format!("failed to bind {address}"))?;
            let common = args.common.resolved();
            let db = open_configured_db(&common)?;
            ingest::recover_interrupted_scan(&db)?;
            let pricing_config = common.pricing();
            let roots = IngestRoots {
                active: common.sessions,
                archive: common.archive,
            };
            let _scanner = (!args.no_ingest).then(|| {
                ingest::spawn_scanner(
                    db.clone(),
                    roots.clone(),
                    Duration::from_secs(args.poll_seconds.max(1)),
                )
            });
            let state = ApiState::new(db.clone(), roots, args.frontend, pricing_config.clone());
            let _pricing_refresher = state.pricing_sync.spawn_refresher(db, pricing_config);
            api::serve(state, listener).await?;
        }
    }
    Ok(())
}

fn open_configured_db(common: &codex_usage::config::CommonArgs) -> Result<Db> {
    match common.pricing_config.as_ref() {
        Some(path) => Db::open_with_pricing_config(&common.db, path),
        None => Db::open(&common.db),
    }
}

#[cfg(unix)]
fn restrict_process_file_creation() {
    // This local application stores private transcripts and usage metadata.
    unsafe {
        libc::umask(0o077);
    }
}

#[cfg(not(unix))]
fn restrict_process_file_creation() {}
