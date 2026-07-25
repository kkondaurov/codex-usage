use super::error::{ApiError, ApiResult};

const MAX_JS_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

pub(crate) fn validated_page(page: Option<u64>) -> ApiResult<u64> {
    let page = page.unwrap_or(1).max(1);
    if page > MAX_JS_SAFE_INTEGER {
        Err(ApiError::bad_request(format!(
            "page must not exceed {MAX_JS_SAFE_INTEGER}"
        )))
    } else {
        Ok(page)
    }
}

pub(crate) fn clamped_page_size(page_size: Option<u64>, default: u64, maximum: u64) -> u64 {
    page_size.unwrap_or(default).clamp(1, maximum)
}

#[cfg(test)]
mod tests {
    use super::MAX_JS_SAFE_INTEGER;
    use super::{clamped_page_size, validated_page};
    use axum::http::StatusCode;

    #[test]
    fn page_defaults_and_zero_normalize_to_one() {
        assert_eq!(validated_page(None).unwrap(), 1);
        assert_eq!(validated_page(Some(0)).unwrap(), 1);
        assert_eq!(
            validated_page(Some(MAX_JS_SAFE_INTEGER)).unwrap(),
            MAX_JS_SAFE_INTEGER
        );
    }

    #[test]
    fn page_rejects_values_javascript_cannot_represent_exactly() {
        let error = validated_page(Some(MAX_JS_SAFE_INTEGER + 1)).unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            error.message(),
            format!("page must not exceed {MAX_JS_SAFE_INTEGER}")
        );
    }

    #[test]
    fn page_size_defaults_and_clamps_to_the_transport_limits() {
        assert_eq!(clamped_page_size(None, 25, 100), 25);
        assert_eq!(clamped_page_size(Some(0), 25, 100), 1);
        assert_eq!(clamped_page_size(Some(101), 25, 100), 100);
    }
}
