"""Tests for scope parsing and resolution."""

import pytest

from synapse_memory.models import Scope, Visibility
from synapse_memory.scope import (
    ScopeParseError,
    is_visible,
    parse_scope,
    scope_contains,
    serialize_scope,
)


class TestParseScope:
    """Test scope path parsing."""

    def test_full_path(self) -> None:
        scope = parse_scope("org:acme/team:support/agent:billing/user:wang/session:abc123")
        assert scope.org == "acme"
        assert scope.team == "support"
        assert scope.agent == "billing"
        assert scope.user == "wang"
        assert scope.session == "abc123"
        assert scope.visibility == Visibility.PRIVATE

    def test_partial_path_org_team(self) -> None:
        scope = parse_scope("org:acme/team:support")
        assert scope.org == "acme"
        assert scope.team == "support"
        assert scope.agent is None
        assert scope.user is None
        assert scope.session is None

    def test_partial_path_user_only(self) -> None:
        scope = parse_scope("user:wang")
        assert scope.org is None
        assert scope.team is None
        assert scope.user == "wang"

    def test_custom_visibility(self) -> None:
        scope = parse_scope("org:acme", visibility=Visibility.SHARED)
        assert scope.org == "acme"
        assert scope.visibility == Visibility.SHARED

    def test_empty_path(self) -> None:
        scope = parse_scope("")
        assert scope.org is None
        assert scope.team is None

    def test_whitespace_handling(self) -> None:
        scope = parse_scope("  org:acme / team:support  ")
        assert scope.org == "acme"
        assert scope.team == "support"

    def test_invalid_no_colon(self) -> None:
        with pytest.raises(ScopeParseError, match="must be in format"):
            parse_scope("acme")

    def test_invalid_unknown_level(self) -> None:
        with pytest.raises(ScopeParseError, match="Unknown scope level"):
            parse_scope("department:engineering")

    def test_invalid_empty_value(self) -> None:
        with pytest.raises(ScopeParseError, match="Empty value"):
            parse_scope("org:")

    def test_invalid_duplicate_level(self) -> None:
        with pytest.raises(ScopeParseError, match="Duplicate scope level"):
            parse_scope("org:acme/org:beta")

    def test_values_with_special_chars(self) -> None:
        scope = parse_scope("org:acme-corp/user:wang.lei@example.com")
        assert scope.org == "acme-corp"
        assert scope.user == "wang.lei@example.com"


class TestSerializeScope:
    """Test scope serialization to path string."""

    def test_full_scope(self) -> None:
        scope = Scope(org="acme", team="support", agent="billing", user="wang", session="abc")
        path = serialize_scope(scope)
        assert path == "org:acme/team:support/agent:billing/user:wang/session:abc"

    def test_partial_scope(self) -> None:
        scope = Scope(org="acme", user="wang")
        path = serialize_scope(scope)
        assert path == "org:acme/user:wang"

    def test_empty_scope(self) -> None:
        scope = Scope()
        path = serialize_scope(scope)
        assert path == ""

    def test_roundtrip(self) -> None:
        original = "org:acme/team:support/user:wang"
        scope = parse_scope(original)
        serialized = serialize_scope(scope)
        assert serialized == original


class TestScopeContains:
    """Test scope containment/hierarchy."""

    def test_parent_contains_child(self) -> None:
        parent = Scope(org="acme")
        child = Scope(org="acme", team="support")
        assert scope_contains(parent, child) is True

    def test_child_does_not_contain_parent(self) -> None:
        parent = Scope(org="acme")
        child = Scope(org="acme", team="support")
        assert scope_contains(child, parent) is False

    def test_same_scope_not_parent(self) -> None:
        scope = Scope(org="acme", team="support")
        assert scope_contains(scope, scope) is False

    def test_different_org_not_parent(self) -> None:
        a = Scope(org="acme")
        b = Scope(org="beta", team="support")
        assert scope_contains(a, b) is False

    def test_deeper_hierarchy(self) -> None:
        org = Scope(org="acme")
        team = Scope(org="acme", team="support")
        agent = Scope(org="acme", team="support", agent="billing")
        assert scope_contains(org, team) is True
        assert scope_contains(org, agent) is True
        assert scope_contains(team, agent) is True


class TestVisibility:
    """Test visibility resolution."""

    def test_public_always_visible(self) -> None:
        record_scope = Scope(org="acme", visibility=Visibility.PUBLIC)
        query_scope = Scope(org="beta")
        assert is_visible(record_scope, query_scope) is True

    def test_private_only_same_scope(self) -> None:
        record_scope = Scope(org="acme", team="support", visibility=Visibility.PRIVATE)
        # Same scope
        same = Scope(org="acme", team="support")
        assert is_visible(record_scope, same) is True
        # Different scope
        diff = Scope(org="acme", team="billing")
        assert is_visible(record_scope, diff) is False

    def test_shared_same_org(self) -> None:
        record_scope = Scope(org="acme", team="support", visibility=Visibility.SHARED)
        # Same org, different team
        same_org = Scope(org="acme", team="billing")
        assert is_visible(record_scope, same_org) is True
        # Different org
        diff_org = Scope(org="beta")
        assert is_visible(record_scope, diff_org) is False

    def test_scope_up_visible_to_parent(self) -> None:
        record_scope = Scope(org="acme", team="support", agent="billing", visibility=Visibility.SCOPE_UP)
        # Parent scope
        parent = Scope(org="acme", team="support")
        assert is_visible(record_scope, parent) is True
        # Grandparent
        grandparent = Scope(org="acme")
        assert is_visible(record_scope, grandparent) is True

    def test_scope_down_visible_to_child(self) -> None:
        record_scope = Scope(org="acme", visibility=Visibility.SCOPE_DOWN)
        # Child scope
        child = Scope(org="acme", team="support")
        assert is_visible(record_scope, child) is True
        # Grandchild
        grandchild = Scope(org="acme", team="support", agent="billing")
        assert is_visible(record_scope, grandchild) is True
