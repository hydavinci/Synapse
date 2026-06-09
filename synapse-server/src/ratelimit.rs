use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::warn;

/// Per-scope rate limiter using a sliding window counter approach.
///
/// Each scope key (e.g. "org:team:agent") gets an independent rate limit.
/// Default: 100 requests per 60 seconds per scope.
///
/// Implements token bucket semantics with lazy cleanup of stale entries.
#[derive(Clone)]
pub struct ScopeRateLimiter {
    state: Arc<RwLock<RateLimitState>>,
    config: RateLimitConfig,
}

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Max requests per window per scope
    pub max_requests: u32,
    /// Window duration
    pub window: Duration,
    /// Max tracked scopes (prevent HashMap explosion from malicious scope sprawl)
    pub max_scopes: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window: Duration::from_secs(60),
            max_scopes: 10_000,
        }
    }
}

struct RateLimitState {
    /// scope_key -> (count, window_start)
    windows: HashMap<String, WindowEntry>,
    /// Last cleanup time
    last_cleanup: Instant,
}

struct WindowEntry {
    count: u32,
    window_start: Instant,
}

impl ScopeRateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(RateLimitState {
                windows: HashMap::new(),
                last_cleanup: Instant::now(),
            })),
            config,
        }
    }

    /// Check if a request for the given scope key is allowed.
    /// Returns Ok(remaining) on success, Err(retry_after_ms) on rate limit.
    pub async fn check(&self, scope_key: &str) -> Result<u32, u64> {
        let mut state = self.state.write().await;
        let now = Instant::now();

        // Periodic cleanup of stale entries (every 5 minutes)
        if now.duration_since(state.last_cleanup) > Duration::from_secs(300) {
            state.windows.retain(|_, entry| {
                now.duration_since(entry.window_start) < self.config.window * 2
            });
            state.last_cleanup = now;
        }

        // Enforce max tracked scopes
        if !state.windows.contains_key(scope_key) && state.windows.len() >= self.config.max_scopes {
            warn!(
                scope = scope_key,
                max = self.config.max_scopes,
                "Rate limiter: max tracked scopes reached, rejecting new scope"
            );
            return Err(self.config.window.as_millis() as u64);
        }

        let entry = state.windows.entry(scope_key.to_string()).or_insert_with(|| {
            WindowEntry {
                count: 0,
                window_start: now,
            }
        });

        // Check if window expired → reset
        if now.duration_since(entry.window_start) >= self.config.window {
            entry.count = 0;
            entry.window_start = now;
        }

        // Check limit
        if entry.count >= self.config.max_requests {
            let elapsed = now.duration_since(entry.window_start);
            let retry_after = self.config.window.saturating_sub(elapsed);
            return Err(retry_after.as_millis() as u64);
        }

        entry.count += 1;
        Ok(self.config.max_requests - entry.count)
    }

    /// Build a scope key from a proto Scope message.
    /// Format: "org:team:agent:user" (empty parts are "_")
    pub fn scope_key(scope: &crate::proto::Scope) -> String {
        format!(
            "{}:{}:{}:{}",
            if scope.org.is_empty() { "_" } else { &scope.org },
            if scope.team.is_empty() { "_" } else { &scope.team },
            if scope.agent.is_empty() { "_" } else { &scope.agent },
            if scope.user.is_empty() { "_" } else { &scope.user },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_allows_within_limit() {
        let limiter = ScopeRateLimiter::new(RateLimitConfig {
            max_requests: 5,
            window: Duration::from_secs(60),
            max_scopes: 100,
        });

        for i in 0..5 {
            let result = limiter.check("test:scope").await;
            assert!(result.is_ok(), "Request {} should be allowed", i);
        }
    }

    #[tokio::test]
    async fn test_rejects_over_limit() {
        let limiter = ScopeRateLimiter::new(RateLimitConfig {
            max_requests: 3,
            window: Duration::from_secs(60),
            max_scopes: 100,
        });

        for _ in 0..3 {
            assert!(limiter.check("test:scope").await.is_ok());
        }

        // 4th request should be rejected
        let result = limiter.check("test:scope").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_independent_scopes() {
        let limiter = ScopeRateLimiter::new(RateLimitConfig {
            max_requests: 2,
            window: Duration::from_secs(60),
            max_scopes: 100,
        });

        assert!(limiter.check("scope:a").await.is_ok());
        assert!(limiter.check("scope:a").await.is_ok());
        assert!(limiter.check("scope:a").await.is_err()); // a is exhausted

        // b should still be fine
        assert!(limiter.check("scope:b").await.is_ok());
    }

    #[tokio::test]
    async fn test_max_scopes_enforcement() {
        let limiter = ScopeRateLimiter::new(RateLimitConfig {
            max_requests: 100,
            window: Duration::from_secs(60),
            max_scopes: 3,
        });

        assert!(limiter.check("scope:1").await.is_ok());
        assert!(limiter.check("scope:2").await.is_ok());
        assert!(limiter.check("scope:3").await.is_ok());
        // 4th unique scope should be rejected
        assert!(limiter.check("scope:4").await.is_err());
    }
}
