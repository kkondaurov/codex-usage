use anyhow::{Context, Result, bail};
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

fn main() -> Result<()> {
    restrict_process_file_creation();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // `tokio::main` drops its runtime after the async entrypoint returns, and
    // that drop waits indefinitely for `spawn_blocking` workers. The HTTP
    // server already gives in-flight work its bounded graceful-drain window;
    // once that boundary expires, process shutdown must not be extended by a
    // worker blocked on SQLite or a cross-process file lock.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to create async runtime")?;
    let result = runtime.block_on(run());
    runtime.shutdown_background();
    result
}

async fn run() -> Result<()> {
    let cli = parse_cli();
    require_repository_root()?;
    match cli.command.expect("the parser always supplies a command") {
        Command::Ingest(args) => {
            let common = args.common.resolved();
            // Opening SQLite performs migrations, seed writes, and manual
            // pricing hydration. Claim ownership from the canonical storage
            // path before any of those shared-state mutations can occur.
            let scanner_lease = ingest::IngestScannerLease::acquire_path(&common.db)?;
            let db = open_configured_db(&common)?;
            ingest::recover_interrupted_scan(&db)?;
            PricingSync::new(DbExecutor::default())
                .sync_if_needed(&db, &common.pricing())
                .await?;
            let report = ingest::scan_one_shot_with_lease(
                &db,
                &IngestRoots {
                    active: common.sessions,
                    archive: common.archive,
                },
                &scanner_lease,
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
            // Retain one ownership lease from startup recovery through any
            // synchronous replay and prewarming, then transfer it directly to
            // the live scanner. Claim it before opening SQLite so a losing
            // process cannot hydrate a different pricing sidecar first.
            let scanner_lease = (!args.no_ingest)
                .then(|| ingest::IngestScannerLease::acquire_path(&common.db))
                .transpose()?;
            let db = open_configured_db(&common)?;
            ingest::recover_interrupted_scan(&db)?;
            let pricing_config = common.pricing();
            let roots = IngestRoots {
                active: common.sessions,
                archive: common.archive,
            };
            if !ingest::projector_generation_is_current(&db)? {
                if args.no_ingest {
                    bail!(
                        "the SQLite projection was built by an older projector; restart without --no-ingest or run `cargo run -- ingest` to rebuild it"
                    );
                }
                tracing::info!(
                    database = %db.path().display(),
                    "rebuilding stale SQLite projection before serving requests"
                );
                ingest::scan_one_shot_with_lease(
                    &db,
                    &roots,
                    scanner_lease
                        .as_ref()
                        .expect("stale projection replay requires ingest ownership"),
                )
                .context("failed to rebuild stale SQLite projection")?;
            }
            api::prewarm_current_year_analytics(&db)?;
            let state = ApiState::new(
                db.clone(),
                roots.clone(),
                args.frontend,
                pricing_config.clone(),
            );
            let scanner = scanner_lease
                .map(|lease| {
                    ingest::spawn_scanner_with_lease(
                        db.clone(),
                        roots.clone(),
                        Duration::from_secs(args.poll_seconds.max(1)),
                        lease,
                    )
                })
                .transpose()?;
            let pricing_refresher = state.pricing_sync.spawn_refresher(db, pricing_config);
            let serve_result = api::serve(state, listener).await;
            if let Some(scanner) = scanner {
                scanner.shutdown();
            }
            pricing_refresher.shutdown().await;
            serve_result?;
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
