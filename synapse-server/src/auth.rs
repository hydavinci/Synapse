use tonic::{Request, Status};
use tracing::warn;

use crate::config::Config;

/// Bearer token authentication interceptor for gRPC services.
///
/// When a token is configured (via SYNAPSE_AUTH_TOKEN env var or config file),
/// all incoming requests must include `authorization: Bearer <token>` metadata.
///
/// If no token is configured, the interceptor passes all requests through (open mode).
#[derive(Clone)]
pub struct AuthInterceptor {
    /// Expected token. None = auth disabled (open mode).
    token: Option<String>,
}

impl AuthInterceptor {
    /// Create interceptor from server config.
    pub fn from_config(config: &Config) -> Self {
        let token = std::env::var("SYNAPSE_AUTH_TOKEN")
            .ok()
            .or_else(|| config.auth_token.clone())
            .filter(|t| !t.is_empty());

        Self { token }
    }

    /// Check if authentication is enabled.
    pub fn is_enabled(&self) -> bool {
        self.token.is_some()
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        // If no token configured, pass everything through
        let expected = match &self.token {
            Some(t) => t,
            None => return Ok(request),
        };

        // Extract authorization header
        let auth_header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());

        match auth_header {
            Some(value) if value.starts_with("Bearer ") => {
                let provided = &value[7..];
                if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
                    Ok(request)
                } else {
                    warn!("Authentication failed: invalid token");
                    Err(Status::unauthenticated("Invalid authentication token"))
                }
            }
            Some(_) => {
                Err(Status::unauthenticated(
                    "Invalid authorization format. Expected: Bearer <token>",
                ))
            }
            None => {
                Err(Status::unauthenticated(
                    "Missing authorization metadata. Required: authorization: Bearer <token>",
                ))
            }
        }
    }
}

/// Constant-time comparison to prevent timing attacks on token validation.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;
    use tonic::service::Interceptor;

    fn make_request(auth: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(value) = auth {
            req.metadata_mut()
                .insert("authorization", MetadataValue::try_from(value).unwrap());
        }
        req
    }

    #[test]
    fn test_no_token_configured_passes_all() {
        let mut interceptor = AuthInterceptor { token: None };
        assert!(interceptor.call(make_request(None)).is_ok());
        assert!(interceptor.call(make_request(Some("Bearer anything"))).is_ok());
    }

    #[test]
    fn test_valid_token() {
        let mut interceptor = AuthInterceptor {
            token: Some("secret123".to_string()),
        };
        assert!(interceptor.call(make_request(Some("Bearer secret123"))).is_ok());
    }

    #[test]
    fn test_invalid_token() {
        let mut interceptor = AuthInterceptor {
            token: Some("secret123".to_string()),
        };
        let result = interceptor.call(make_request(Some("Bearer wrong")));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn test_missing_token() {
        let mut interceptor = AuthInterceptor {
            token: Some("secret123".to_string()),
        };
        let result = interceptor.call(make_request(None));
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_format() {
        let mut interceptor = AuthInterceptor {
            token: Some("secret123".to_string()),
        };
        let result = interceptor.call(make_request(Some("Basic dXNlcjpwYXNz")));
        assert!(result.is_err());
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"short", b"longer_string"));
    }
}
