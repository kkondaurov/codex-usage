use super::boundary::browser_boundary;
use anyhow::{Context, Result};
use axum::{Router, http::StatusCode, middleware, response::IntoResponse};
use std::{future::IntoFuture, path::PathBuf, time::Duration};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

pub fn application_router(api: Router, frontend: PathBuf) -> Router {
    let mut app = Router::new().nest("/api/v1", api);
    let index = frontend.join("index.html");
    if index.is_file() {
        app = app.fallback_service(ServeDir::new(frontend).fallback(ServeFile::new(index)));
    } else {
        app = app.fallback(frontend_missing);
    }
    app.layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(browser_boundary))
}

pub async fn serve(app: Router, listener: tokio::net::TcpListener) -> Result<()> {
    let address = listener
        .local_addr()
        .context("failed to inspect bound listener")?;
    tracing::info!(%address, "Codex Usage is ready");
    let (begin_shutdown, shutdown_requested) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_requested.await;
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        _ = shutdown_signal() => {
            // Stop accepting immediately, then give well-behaved in-flight
            // requests a short window to finish. An idle keep-alive socket or
            // an incomplete HTTP header must not hold the local process open
            // forever during shutdown.
            let _ = begin_shutdown.send(());
            match tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, &mut server).await {
                Ok(result) => result?,
                Err(_) => {
                    tracing::warn!(
                        timeout_ms = GRACEFUL_SHUTDOWN_TIMEOUT.as_millis(),
                        "forcing server shutdown after graceful drain deadline"
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler must install");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn frontend_missing() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        "Frontend build not found. Run `npm run build` in frontend/.",
    )
}

#[cfg(test)]
mod tests {
    use super::application_router;
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, StatusCode, header::CONTENT_TYPE},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn missing_frontend_returns_the_existing_plain_text_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let app = application_router(Router::new(), temp.path().join("missing"));
        let response = app
            .oneshot(
                Request::get("/")
                    .header("host", "127.0.0.1:5610")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(
            response.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/plain")
        );
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        assert_eq!(
            body.as_ref(),
            b"Frontend build not found. Run `npm run build` in frontend/."
        );
    }
}
