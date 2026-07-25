use crate::{
    activity, analytics,
    config::PricingConfig,
    ingest::IngestRoots,
    pricing::{self, ManualPricingStore, PricingMutations, PricingSync},
    sessions,
    storage::{Db, StorageExecutor},
    system,
    web::{
        ReadRuntime,
        error::{api_error_contract, api_not_found},
        server,
    },
};
use axum::{Router, middleware};
use std::path::PathBuf;

/// The process-owned dependencies required to assemble the HTTP application.
///
/// Construction is explicit so the binary and integration harnesses share the
/// same executor and pricing synchronization topology. Building consumes the
/// dependencies; there is no cloneable application service locator.
pub struct AppDependencies {
    database: Db,
    roots: IngestRoots,
    frontend: PathBuf,
    pricing: PricingConfig,
    executor: StorageExecutor,
    pricing_sync: PricingSync,
    manual_pricing: ManualPricingStore,
}

impl AppDependencies {
    pub fn new(
        database: Db,
        roots: IngestRoots,
        frontend: PathBuf,
        pricing: PricingConfig,
        executor: StorageExecutor,
        pricing_sync: PricingSync,
        manual_pricing: ManualPricingStore,
    ) -> Self {
        Self {
            database,
            roots,
            frontend,
            pricing,
            executor,
            pricing_sync,
            manual_pricing,
        }
    }

    pub fn build(self) -> Router {
        let reads = ReadRuntime::new(self.database.clone(), self.executor.clone());
        let active_root = self.roots.active;
        let archive_root = self.roots.archive;
        let pricing_mutations =
            PricingMutations::new(self.database.clone(), self.manual_pricing, self.executor);
        let api = Router::new()
            .merge(system::router(reads.clone(), active_root, archive_root))
            .merge(sessions::router(reads.clone()))
            .merge(activity::router(reads.clone()))
            .merge(analytics::router(reads.clone()))
            .merge(pricing::router(
                reads,
                self.database,
                self.pricing,
                self.pricing_sync,
                pricing_mutations,
            ))
            .fallback(api_not_found)
            .layer(middleware::from_fn(api_error_contract));
        server::application_router(api, self.frontend)
    }
}
