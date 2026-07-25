use axum::{
    Json,
    body::{Body as AxumBody, to_bytes},
    http::{
        Request, StatusCode,
        header::{ALLOW, CONTENT_TYPE},
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;

pub(crate) async fn api_error_contract(request: Request<AxumBody>, next: Next) -> Response {
    let response = next.run(request).await;
    if !response.status().is_client_error()
        || response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        return response;
    }

    let status = response.status();
    let allow = response.headers().get(ALLOW).cloned();
    let message = to_bytes(response.into_body(), 16 * 1024)
        .await
        .ok()
        .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok())
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| {
            status
                .canonical_reason()
                .unwrap_or("invalid API request")
                .to_owned()
        });
    let mut response = ApiError::new(status, message).into_response();
    if let Some(allow) = allow {
        response.headers_mut().insert(ALLOW, allow);
    }
    response
}

pub(crate) async fn api_not_found() -> ApiError {
    ApiError::not_found("API route not found")
}

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    pub(crate) fn bad_gateway(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, message)
    }

    pub(crate) fn internal(error: impl std::fmt::Display) -> Self {
        tracing::warn!(%error, "API request failed");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }

    #[cfg(test)]
    pub(crate) fn status(&self) -> StatusCode {
        self.status
    }

    #[cfg(test)]
    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Body {
            error: String,
        }
        (
            self.status,
            Json(Body {
                error: self.message,
            }),
        )
            .into_response()
    }
}

pub(crate) type ApiResult<T> = std::result::Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::api_error_contract;
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, StatusCode, header::CONTENT_TYPE},
        middleware,
        response::Response,
        routing::get,
    };
    use serde_json::Value;
    use tower::ServiceExt;

    #[tokio::test]
    async fn plain_text_server_errors_pass_through_unchanged() {
        let app = Router::new()
            .route(
                "/",
                get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "plain failure") }),
            )
            .layer(middleware::from_fn(api_error_contract));

        let response = app
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            response.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/plain")
        );
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        assert_eq!(body.as_ref(), b"plain failure");
    }

    #[tokio::test]
    async fn oversized_client_error_uses_the_canonical_reason() {
        let app = Router::new()
            .route(
                "/",
                get(|| async {
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(vec![b'x'; 16 * 1024 + 1]))
                        .unwrap()
                }),
            )
            .layer(middleware::from_fn(api_error_contract));

        let response = app
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            response.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"], "Bad Request");
    }
}
