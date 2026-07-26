use anyhow::{Context, Result, bail};
use codex_usage::{
    analytics,
    app::AppDependencies,
    config::{Command, parse_cli, require_repository_root},
    ingest::{self, IngestRoots},
    pricing::{ManualPricingStore, PricingSync},
    storage::{DatabaseLocation, Db, StorageExecutor},
    web::server,
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
            // Claim scanner ownership before any database or pricing-state
            // mutation can occur.
            let scanner_lease = ingest::IngestScannerLease::acquire_path(&common.db)?;
            let (db, _manual_pricing) = open_configured_db(&common)?;
            ingest::recover_interrupted_scan(&db)?;
            PricingSync::new(StorageExecutor::default())
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
            // Every server is write-capable: even `--no-ingest` performs
            // recovery, hydrates manual pricing, exposes pricing mutations,
            // and runs the pricing refresher. Retain one projection ownership
            // lease for the server's lifetime so another port cannot attach a
            // conflicting configuration to the same database. When scanning
            // is enabled, transfer that lease directly to the live scanner.
            let projection_lease = ingest::IngestScannerLease::acquire_path(&common.db)?;
            let (db, manual_pricing) = open_configured_db(&common)?;
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
                ingest::scan_one_shot_with_lease(&db, &roots, &projection_lease)
                    .context("failed to rebuild stale SQLite projection")?;
            }
            analytics::prewarm_current_year_analytics(&db)?;
            let frontend = args.frontend.clone();
            let executor = StorageExecutor::default();
            let pricing_sync = PricingSync::new(executor.clone());
            let scanner = if args.no_ingest {
                None
            } else {
                Some(
                    ingest::spawn_scanner_with_lease(
                        db.clone(),
                        roots.clone(),
                        Duration::from_secs(args.poll_seconds.max(1)),
                        projection_lease,
                    )
                    .context("failed to start live ingest scanner")?,
                )
            };
            let pricing_refresher =
                pricing_sync.spawn_refresher(db.clone(), pricing_config.clone());
            let app = AppDependencies::new(
                db,
                roots,
                frontend,
                pricing_config,
                executor,
                pricing_sync,
                manual_pricing,
            )
            .build();
            let serve_result = server::serve(app, listener).await;
            if let Some(scanner) = scanner {
                scanner.shutdown();
            }
            pricing_refresher.shutdown().await;
            serve_result?;
        }
    }
    Ok(())
}

fn open_configured_db(
    common: &codex_usage::config::CommonArgs,
) -> Result<(Db, ManualPricingStore)> {
    let location = DatabaseLocation::prepare(&common.db)?;
    let pricing_path = common
        .pricing_config
        .clone()
        .unwrap_or_else(|| location.path().with_extension("pricing.json"));
    let manual_pricing = ManualPricingStore::new(pricing_path)?;
    let db = location.open()?;
    manual_pricing.hydrate(&db)?;
    Ok((db, manual_pricing))
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
