"""Utility functions for the Synapse Memory Protocol SDK.

Includes context formatting, embedding helpers, and scope path utilities.
"""

from __future__ import annotations

from typing import Optional

from .models import MemoryKind, SearchResult


def to_context(
    results: list[SearchResult],
    *,
    max_tokens: int = 2000,
    include_metadata: bool = True,
    separator: str = "\n---\n",
) -> str:
    """Format search results into an LLM-friendly context string.

    Produces a readable text block suitable for injection into LLM prompts.
    Respects approximate token limits (using ~4 chars per token heuristic).

    Args:
        results: List of SearchResult objects to format
        max_tokens: Approximate maximum token count (default: 2000)
        include_metadata: Whether to include tags, kind, confidence
        separator: Separator between memory entries

    Returns:
        Formatted context string

    Example:
        >>> context = to_context(results, max_tokens=1000)
        >>> prompt = f"Using the following context:\\n{context}\\n\\nAnswer: ..."
    """
    if not results:
        return ""

    # Approximate char budget (4 chars ≈ 1 token)
    max_chars = max_tokens * 4
    output_parts: list[str] = []
    current_chars = 0

    header = f"[{len(results)} memories found]\n"
    current_chars += len(header)
    output_parts.append(header)

    for i, result in enumerate(results, 1):
        record = result.record
        score = result.score

        # Build entry
        entry_parts: list[str] = []

        # Header line
        entry_parts.append(f"[{i}] (score: {score:.2f})")

        # Content
        entry_parts.append(record.content)

        # Metadata (optional)
        if include_metadata:
            meta_parts: list[str] = []
            if record.kind and record.kind != MemoryKind.FACT:
                meta_parts.append(f"kind:{record.kind.value}")
            if record.tags:
                meta_parts.append(f"tags:{','.join(record.tags)}")
            if record.confidence < 1.0:
                meta_parts.append(f"confidence:{record.confidence:.1f}")
            if record.scope.to_path():
                meta_parts.append(f"scope:{record.scope.to_path()}")
            if meta_parts:
                entry_parts.append(f"  [{' | '.join(meta_parts)}]")

        entry = "\n".join(entry_parts)
        entry_with_sep = separator + entry if output_parts else entry
        entry_len = len(entry_with_sep)

        # Check budget
        if current_chars + entry_len > max_chars:
            # Try to fit a truncated version
            remaining = max_chars - current_chars - len(separator) - 50
            if remaining > 100:
                truncated = entry[:remaining] + "..."
                output_parts.append(separator + truncated)
            remaining_count = len(results) - i
            if remaining_count > 0:
                output_parts.append(f"\n[...{remaining_count} more results truncated]")
            break

        output_parts.append(entry_with_sep)
        current_chars += entry_len

    return "".join(output_parts)


def estimate_tokens(text: str) -> int:
    """Estimate token count for a text string.

    Uses a simple heuristic of ~4 characters per token.
    For more accurate counts, use tiktoken or the model's tokenizer.

    Args:
        text: Input text

    Returns:
        Estimated token count
    """
    return max(1, len(text) // 4)


def truncate_to_tokens(text: str, max_tokens: int) -> str:
    """Truncate text to approximately max_tokens.

    Args:
        text: Input text
        max_tokens: Maximum token count

    Returns:
        Truncated text with ellipsis if truncated
    """
    max_chars = max_tokens * 4
    if len(text) <= max_chars:
        return text
    return text[:max_chars - 3] + "..."


def format_memory_record(
    content: str,
    kind: Optional[MemoryKind] = None,
    tags: Optional[list[str]] = None,
    confidence: Optional[float] = None,
) -> str:
    """Format a memory record for display.

    Args:
        content: Memory content
        kind: Memory kind
        tags: Memory tags
        confidence: Confidence score

    Returns:
        Formatted string
    """
    parts = [content]
    meta: list[str] = []
    if kind:
        meta.append(f"[{kind.value}]")
    if tags:
        meta.append(f"tags: {', '.join(tags)}")
    if confidence is not None and confidence < 1.0:
        meta.append(f"confidence: {confidence:.0%}")
    if meta:
        parts.append(f"  ({' | '.join(meta)})")
    return "\n".join(parts)
