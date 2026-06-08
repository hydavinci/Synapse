"""Embedding providers for local vector search.

Supports multiple embedding backends:
- OpenAI (text-embedding-3-small/large)
- Local sentence-transformers (no API key needed)
- Custom callable

Users can pick based on their needs:
- No API key? Use local sentence-transformers (slower first load, free forever)
- Have OpenAI key? Use OpenAI embeddings (fast, high quality)
- Custom? Pass any callable that returns list[float]
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# Type alias for embedding functions
EmbeddingFn = Callable[[str], list[float]]


def openai_embedding(
    model: str = "text-embedding-3-small",
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> EmbeddingFn:
    """Create an OpenAI embedding function.

    Args:
        model: OpenAI embedding model name
        api_key: API key (falls back to OPENAI_API_KEY env var)
        base_url: Custom API base URL (for compatible APIs like Azure, local)

    Returns:
        A callable that takes text and returns embedding vector.
    """
    import os

    key = api_key or os.environ.get("OPENAI_API_KEY")
    if not key:
        raise ValueError(
            "OpenAI API key required. Set OPENAI_API_KEY env var or pass api_key parameter."
        )

    def _embed(text: str) -> list[float]:
        import httpx

        url = (base_url or "https://api.openai.com/v1") + "/embeddings"
        resp = httpx.post(
            url,
            headers={"Authorization": f"Bearer {key}"},
            json={"input": text, "model": model},
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["data"][0]["embedding"]

    return _embed


def local_embedding(
    model_name: str = "all-MiniLM-L6-v2",
) -> EmbeddingFn:
    """Create a local sentence-transformers embedding function.

    First call downloads the model (~80MB for MiniLM). Subsequent calls are fast.
    No API key needed.

    Args:
        model_name: sentence-transformers model name

    Returns:
        A callable that takes text and returns embedding vector.
    """
    _model = None

    def _embed(text: str) -> list[float]:
        nonlocal _model
        if _model is None:
            try:
                from sentence_transformers import SentenceTransformer

                logger.info("Loading embedding model '%s' (first time may download)...", model_name)
                _model = SentenceTransformer(model_name)
                logger.info("Embedding model loaded.")
            except ImportError:
                raise ImportError(
                    "sentence-transformers required for local embeddings. "
                    "Install with: pip install synapse-memory[local]"
                )

        embedding = _model.encode(text, convert_to_numpy=True)
        return embedding.tolist()

    return _embed


def auto_embedding() -> Optional[EmbeddingFn]:
    """Auto-detect the best available embedding provider.

    Priority:
    1. OpenAI if OPENAI_API_KEY is set
    2. Local sentence-transformers if installed
    3. None (keyword search only)
    """
    import os

    if os.environ.get("OPENAI_API_KEY"):
        logger.info("Using OpenAI embeddings (OPENAI_API_KEY detected)")
        return openai_embedding()

    try:
        import sentence_transformers  # noqa: F401

        logger.info("Using local sentence-transformers embeddings")
        return local_embedding()
    except ImportError:
        pass

    logger.info("No embedding provider available. Using keyword search only.")
    logger.info("For better search: pip install synapse-memory[local] or set OPENAI_API_KEY")
    return None
