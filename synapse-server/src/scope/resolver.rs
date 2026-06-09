use crate::proto::{self, Visibility};

/// Resolves scope visibility rules as defined in the protocol spec §2.3.
///
/// A query at scope {org: "acme", team: "support"} sees:
/// - All records with matching scope
/// - All records with visibility: SCOPE_DOWN from parent {org: "acme"}
/// - All records with visibility: SCOPE_UP from children (e.g., specific agents in support team)
/// - All SHARED records within org "acme"
/// - All PUBLIC records
pub struct ScopeResolver;

impl ScopeResolver {
    /// Check if a record is visible to a querying scope.
    pub fn is_visible(record_scope: &proto::Scope, query_scope: &proto::Scope) -> bool {
        let visibility =
            Visibility::try_from(record_scope.visibility).unwrap_or(Visibility::Private);

        match visibility {
            Visibility::Public => {
                // PUBLIC: visible to everyone
                true
            }
            Visibility::Shared => {
                // SHARED: visible to all within the same org
                Self::same_org(record_scope, query_scope)
            }
            Visibility::Private => {
                // PRIVATE: only the owner scope can access
                Self::exact_match(record_scope, query_scope)
            }
            Visibility::ScopeUp => {
                // SCOPE_UP: visible to parent scopes
                // The record is at a child scope, visible to queries at parent scope
                Self::exact_match(record_scope, query_scope)
                    || Self::is_parent_of(query_scope, record_scope)
            }
            Visibility::ScopeDown => {
                // SCOPE_DOWN: visible to child scopes
                // The record is at a parent scope, visible to queries at child scope
                Self::exact_match(record_scope, query_scope)
                    || Self::is_parent_of(record_scope, query_scope)
            }
            Visibility::Unspecified => {
                // Treat unspecified as PRIVATE
                Self::exact_match(record_scope, query_scope)
            }
        }
    }

    /// Check if two scopes are in the same org.
    fn same_org(a: &proto::Scope, b: &proto::Scope) -> bool {
        !a.org.is_empty() && a.org == b.org
    }

    /// Check if scopes match exactly (ignoring visibility field).
    /// Empty query fields are treated as wildcards.
    fn exact_match(record_scope: &proto::Scope, query_scope: &proto::Scope) -> bool {
        let matches_org = query_scope.org.is_empty() || record_scope.org == query_scope.org;
        let matches_team = query_scope.team.is_empty() || record_scope.team == query_scope.team;
        let matches_agent = query_scope.agent.is_empty() || record_scope.agent == query_scope.agent;
        let matches_user = query_scope.user.is_empty() || record_scope.user == query_scope.user;
        let matches_session =
            query_scope.session.is_empty() || record_scope.session == query_scope.session;

        matches_org && matches_team && matches_agent && matches_user && matches_session
    }

    /// Check if `parent` is a parent scope of `child`.
    /// A scope is a parent if it has fewer specified levels and the specified ones match.
    ///
    /// Hierarchy: org > team > agent > user > session
    fn is_parent_of(parent: &proto::Scope, child: &proto::Scope) -> bool {
        // Parent must have at least org match
        if parent.org.is_empty() && child.org.is_empty() {
            return false;
        }
        if !parent.org.is_empty() && parent.org != child.org {
            return false;
        }

        // Walk down the hierarchy. Parent stops at a higher level than child.
        let parent_depth = Self::scope_depth(parent);
        let child_depth = Self::scope_depth(child);

        if parent_depth >= child_depth {
            return false; // Parent must be at a higher (less specific) level
        }

        // All specified parent fields must match the corresponding child fields
        if !parent.org.is_empty() && parent.org != child.org {
            return false;
        }
        if !parent.team.is_empty() && parent.team != child.team {
            return false;
        }
        if !parent.agent.is_empty() && parent.agent != child.agent {
            return false;
        }
        if !parent.user.is_empty() && parent.user != child.user {
            return false;
        }

        true
    }

    /// Calculate the depth of a scope (how many levels are specified).
    fn scope_depth(scope: &proto::Scope) -> u8 {
        let mut depth = 0;
        if !scope.org.is_empty() {
            depth += 1;
        }
        if !scope.team.is_empty() {
            depth += 1;
        }
        if !scope.agent.is_empty() {
            depth += 1;
        }
        if !scope.user.is_empty() {
            depth += 1;
        }
        if !scope.session.is_empty() {
            depth += 1;
        }
        depth
    }

    /// Filter a list of records by scope visibility.
    pub fn filter_visible(
        records: &[proto::MemoryRecord],
        query_scope: &proto::Scope,
    ) -> Vec<proto::MemoryRecord> {
        records
            .iter()
            .filter(|r| {
                if let Some(ref scope) = r.scope {
                    Self::is_visible(scope, query_scope)
                } else {
                    false
                }
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scope(org: &str, team: &str, agent: &str, vis: i32) -> proto::Scope {
        proto::Scope {
            org: org.to_string(),
            team: team.to_string(),
            agent: agent.to_string(),
            user: String::new(),
            session: String::new(),
            visibility: vis,
        }
    }

    #[test]
    fn test_public_always_visible() {
        let record_scope = make_scope("acme", "support", "", Visibility::Public as i32);
        let query_scope = make_scope("other_org", "", "", 0);
        assert!(ScopeResolver::is_visible(&record_scope, &query_scope));
    }

    #[test]
    fn test_shared_same_org() {
        let record_scope = make_scope("acme", "support", "", Visibility::Shared as i32);
        let query_scope = make_scope("acme", "engineering", "", 0);
        assert!(ScopeResolver::is_visible(&record_scope, &query_scope));
    }

    #[test]
    fn test_shared_different_org() {
        let record_scope = make_scope("acme", "support", "", Visibility::Shared as i32);
        let query_scope = make_scope("other", "support", "", 0);
        assert!(!ScopeResolver::is_visible(&record_scope, &query_scope));
    }

    #[test]
    fn test_private_exact_match() {
        let record_scope = make_scope("acme", "support", "bot1", Visibility::Private as i32);
        let query_scope = make_scope("acme", "support", "bot1", 0);
        assert!(ScopeResolver::is_visible(&record_scope, &query_scope));
    }

    #[test]
    fn test_private_no_match() {
        let record_scope = make_scope("acme", "support", "bot1", Visibility::Private as i32);
        let query_scope = make_scope("acme", "support", "bot2", 0);
        assert!(!ScopeResolver::is_visible(&record_scope, &query_scope));
    }

    #[test]
    fn test_scope_down_parent_to_child() {
        // Record at org level with SCOPE_DOWN should be visible to team level
        let record_scope = make_scope("acme", "", "", Visibility::ScopeDown as i32);
        let query_scope = make_scope("acme", "support", "", 0);
        assert!(ScopeResolver::is_visible(&record_scope, &query_scope));
    }

    #[test]
    fn test_scope_up_child_to_parent() {
        // Record at agent level with SCOPE_UP should be visible to team level
        let record_scope = make_scope("acme", "support", "bot1", Visibility::ScopeUp as i32);
        let query_scope = make_scope("acme", "support", "", 0);
        assert!(ScopeResolver::is_visible(&record_scope, &query_scope));
    }
}
