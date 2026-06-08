"""Scope parsing and resolution for the Synapse Memory Protocol.

Scope paths use the syntax: 'org:acme/team:support/agent:billing/user:wang/session:abc123'
Partial paths are valid: 'user:wang' means any org/team/agent for that user.
"""

from __future__ import annotations

from .models import Scope, Visibility


# Valid scope segment types in hierarchical order
SCOPE_LEVELS: list[str] = ["org", "team", "agent", "user", "session"]


class ScopeParseError(ValueError):
    """Raised when a scope path string cannot be parsed."""

    pass


def parse_scope(path: str, visibility: Visibility = Visibility.PRIVATE) -> Scope:
    """Parse a scope path string into a Scope object.

    Args:
        path: Scope path string, e.g. 'org:acme/team:support/user:wang'
        visibility: Visibility level to assign (default: PRIVATE)

    Returns:
        Parsed Scope object

    Raises:
        ScopeParseError: If the path is malformed

    Examples:
        >>> parse_scope("org:acme/team:support")
        Scope(org='acme', team='support', visibility='private')
        >>> parse_scope("user:wang")
        Scope(user='wang', visibility='private')
    """
    if not path or not path.strip():
        return Scope(visibility=visibility)

    fields: dict[str, str] = {}
    segments = path.strip().split("/")

    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue

        if ":" not in segment:
            raise ScopeParseError(
                f"Invalid scope segment '{segment}': must be in format 'level:value'"
            )

        level, value = segment.split(":", 1)
        level = level.strip().lower()
        value = value.strip()

        if level not in SCOPE_LEVELS:
            raise ScopeParseError(
                f"Unknown scope level '{level}'. Valid levels: {SCOPE_LEVELS}"
            )

        if not value:
            raise ScopeParseError(
                f"Empty value for scope level '{level}'"
            )

        if level in fields:
            raise ScopeParseError(
                f"Duplicate scope level '{level}' in path"
            )

        fields[level] = value

    return Scope(visibility=visibility, **fields)


def serialize_scope(scope: Scope) -> str:
    """Serialize a Scope object back to path string.

    Args:
        scope: Scope object to serialize

    Returns:
        Scope path string, e.g. 'org:acme/team:support'
    """
    return scope.to_path()


def scope_contains(parent: Scope, child: Scope) -> bool:
    """Check if a parent scope contains a child scope.

    A scope A contains B if all fields set in A are also set in B with the same values,
    and B has additional fields set (is more specific).

    Args:
        parent: The potentially broader scope
        child: The potentially narrower scope

    Returns:
        True if parent contains child
    """
    return parent.is_parent_of(child)


def is_visible(record_scope: Scope, query_scope: Scope) -> bool:
    """Determine if a record with record_scope is visible from query_scope.

    Visibility rules:
    - PRIVATE: Only visible if scopes match exactly
    - SCOPE_UP: Visible to parent scopes (agent→team→org)
    - SCOPE_DOWN: Visible to child scopes (org→team→agent)
    - SHARED: Visible to all within same org
    - PUBLIC: Visible to everyone

    Args:
        record_scope: The scope of the memory record
        query_scope: The scope of the query/requester

    Returns:
        True if the record is visible from the query scope
    """
    visibility = record_scope.visibility

    if visibility == Visibility.PUBLIC:
        return True

    if visibility == Visibility.SHARED:
        # Visible within same org
        if record_scope.org and query_scope.org:
            return record_scope.org == query_scope.org
        # If no org specified, shared is visible to all
        return not record_scope.org

    if visibility == Visibility.PRIVATE:
        # Must match exactly (all set fields are equal)
        return record_scope.matches(query_scope) and query_scope.matches(record_scope)

    if visibility == Visibility.SCOPE_UP:
        # Visible to parent scopes: if query is parent of record, it can see it
        return query_scope.is_parent_of(record_scope) or record_scope.matches(query_scope)

    if visibility == Visibility.SCOPE_DOWN:
        # Visible to child scopes: if query is child of record, it can see it
        return record_scope.is_parent_of(query_scope) or record_scope.matches(query_scope)

    return False


def resolve_visible_scopes(query_scope: Scope) -> list[Visibility]:
    """Return which visibility levels a query scope can access.

    Args:
        query_scope: The scope making the query

    Returns:
        List of Visibility levels the query can see
    """
    # Everyone can see PUBLIC
    visible = [Visibility.PUBLIC]

    # Own private records
    visible.append(Visibility.PRIVATE)

    # If in an org, can see SHARED within that org
    if query_scope.org:
        visible.append(Visibility.SHARED)

    # Can see SCOPE_UP from children
    visible.append(Visibility.SCOPE_UP)

    # Can see SCOPE_DOWN from parents
    visible.append(Visibility.SCOPE_DOWN)

    return visible
