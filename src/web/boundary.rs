use super::error::ApiError;
use axum::{
    body::Body as AxumBody,
    http::{
        HeaderName, HeaderValue, Method, Request, Uri,
        header::{HOST, ORIGIN},
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{net::IpAddr, str::FromStr};

pub(crate) async fn browser_boundary(request: Request<AxumBody>, next: Next) -> Response {
    let host = match request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
    {
        Some(host) if is_loopback_authority(host) => host.to_owned(),
        _ => return boundary_rejection("request host must be localhost or a loopback address"),
    };
    let fetch_site = request
        .headers()
        .get(HeaderName::from_static("sec-fetch-site"))
        .and_then(|value| value.to_str().ok());
    if is_mutating_method(request.method()) {
        if fetch_site.is_some_and(|site| !matches!(site, "same-origin" | "none")) {
            return boundary_rejection("cross-origin mutations are not allowed");
        }
        if let Some(origin) = request.headers().get(ORIGIN) {
            let allowed = origin
                .to_str()
                .ok()
                .and_then(|origin| Uri::from_str(origin).ok())
                .is_some_and(|origin| {
                    matches!(origin.scheme_str(), Some("http" | "https"))
                        && origin.authority().is_some_and(|authority| {
                            is_loopback_authority(authority.as_str())
                                && authority.as_str().eq_ignore_ascii_case(&host)
                        })
                });
            if !allowed {
                return boundary_rejection("mutation origin does not match the local application");
            }
        }
    } else if is_api_path(request.uri().path())
        && fetch_site.is_some_and(|site| !matches!(site, "same-origin" | "none"))
    {
        return boundary_rejection("cross-origin API requests are not allowed");
    }

    let mut response = next.run(request).await;
    let content_security_policy = response
        .headers()
        .get(HeaderName::from_static("content-security-policy"))
        .and_then(|value| value.to_str().ok())
        .map(|value| format!("{value}; frame-ancestors 'none'"))
        .unwrap_or_else(|| "frame-ancestors 'none'".to_owned());
    if let Ok(value) = HeaderValue::from_str(&content_security_policy) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("content-security-policy"), value);
    }
    response.headers_mut().insert(
        HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    response
}

fn is_loopback_authority(value: &str) -> bool {
    let Ok(authority) = axum::http::uri::Authority::from_str(value) else {
        return false;
    };
    let host = authority.host().trim_matches(['[', ']']);
    host.eq_ignore_ascii_case("localhost")
        || IpAddr::from_str(host).is_ok_and(|address| address.is_loopback())
}

fn is_mutating_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn is_api_path(path: &str) -> bool {
    path == "/api/v1" || path.starts_with("/api/v1/")
}

fn boundary_rejection(message: &'static str) -> Response {
    ApiError::forbidden(message).into_response()
}

#[cfg(test)]
mod tests {
    use super::{browser_boundary, is_api_path, is_loopback_authority, is_mutating_method};
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{
            HeaderName, HeaderValue, Method, Request, StatusCode,
            header::{HOST, ORIGIN},
        },
        middleware,
        response::Response,
        routing::{get, post},
    };
    use serde_json::Value;
    use tower::ServiceExt;

    #[tokio::test]
    async fn mutation_origin_must_match_the_complete_loopback_authority() {
        let app = Router::new()
            .route("/", post(|| async { StatusCode::NO_CONTENT }))
            .layer(middleware::from_fn(browser_boundary));

        for origin in ["http://localhost:5610", "http://127.0.0.1:5611"] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/")
                        .header(HOST, "127.0.0.1:5610")
                        .header(ORIGIN, origin)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "origin={origin}");
            let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap()["error"],
                "mutation origin does not match the local application"
            );
        }

        let response = app
            .oneshot(
                Request::post("/")
                    .header(HOST, "127.0.0.1:5610")
                    .header(ORIGIN, "http://127.0.0.1:5610")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn existing_content_security_policy_is_preserved_and_frame_denied() {
        let content_security_policy = HeaderName::from_static("content-security-policy");
        let app = Router::new()
            .route(
                "/",
                get(move || {
                    let content_security_policy = content_security_policy.clone();
                    async move {
                        let mut response = Response::new(Body::empty());
                        response.headers_mut().insert(
                            content_security_policy,
                            HeaderValue::from_static("default-src 'self'"),
                        );
                        response
                    }
                }),
            )
            .layer(middleware::from_fn(browser_boundary));

        let response = app
            .oneshot(
                Request::get("/")
                    .header(HOST, "127.0.0.1:5610")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-security-policy"],
            "default-src 'self'; frame-ancestors 'none'"
        );
        assert_eq!(response.headers()["x-frame-options"], "DENY");
    }

    #[test]
    fn helper_scope_remains_narrow() {
        for authority in [
            "localhost",
            "LOCALHOST:5610",
            "127.0.0.1:5610",
            "[::1]:5610",
        ] {
            assert!(is_loopback_authority(authority), "authority={authority}");
        }
        for authority in ["example.test:5610", "192.168.1.1:5610", "not a host"] {
            assert!(!is_loopback_authority(authority), "authority={authority}");
        }

        for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
            assert!(is_mutating_method(&method), "method={method}");
        }
        for method in [Method::GET, Method::HEAD, Method::OPTIONS] {
            assert!(!is_mutating_method(&method), "method={method}");
        }

        for path in ["/api/v1", "/api/v1/status"] {
            assert!(is_api_path(path), "path={path}");
        }
        for path in ["/", "/api/v10", "/api/v1evil"] {
            assert!(!is_api_path(path), "path={path}");
        }
    }
}
