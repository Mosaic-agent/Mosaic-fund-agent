"""
src/db/db_metadata_rag.py
───────────────────────────
Manages semantic indexing and retrieval of ClickHouse table schemas and SQL query
templates using Qdrant. Reduces agent prompt size by dynamically injecting schema details.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Optional

log = logging.getLogger(__name__)

# Lazy imports for Qdrant client
_qdrant_available = True
try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
except ImportError:
    _qdrant_available = False

_qdrant_client: Optional[QdrantClient] = None
_collection_verified = False
COLLECTION_NAME = "clickhouse_metadata"

def get_qdrant_client() -> Optional[QdrantClient]:
    """Lazy initialize Qdrant client."""
    global _qdrant_client
    if not _qdrant_available:
        return None
    if _qdrant_client is None:
        try:
            from config.settings import settings
            host = os.environ.get("QDRANT_HOST") or getattr(settings, "qdrant_host", "localhost")
            port = int(os.environ.get("QDRANT_PORT") or getattr(settings, "qdrant_port", 6333))
            grpc_port = int(os.environ.get("QDRANT_GRPC_PORT") or getattr(settings, "qdrant_grpc_port", 6334))
            from src.utils.net_check import is_port_open
            if not is_port_open(host, port):
                log.debug("Qdrant unreachable at %s:%s — skipping metadata client init", host, port)
                return None
            _qdrant_client = QdrantClient(host=host, port=port, grpc_port=grpc_port, prefer_grpc=True, timeout=10.0)
        except Exception as e:
            log.debug("Failed to initialize QdrantClient for metadata: %s", e)
            _qdrant_client = None
    return _qdrant_client

def ensure_metadata_collection(client: QdrantClient, dim: int = 768) -> bool:
    """Ensure the clickhouse_metadata collection exists."""
    global _collection_verified
    if _collection_verified:
        return True
    try:
        collections = client.get_collections().collections
        exists = any(c.name == COLLECTION_NAME for c in collections)
        if not exists:
            client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            )
            log.info("Created Qdrant collection: %s", COLLECTION_NAME)
        _collection_verified = True
        return True
    except Exception as e:
        log.warning("Qdrant metadata collection check failed: %s", e)
        return False

def index_metadata_points(points_data: list[dict[str, Any]]):
    """
    Index table schemas or SQL query templates in Qdrant clickhouse_metadata collection.
    
    Each points_data dict should have:
      - 'type': 'table_schema' or 'sql_template'
      - 'name': name of the table or name of the template
      - 'description': explanation of what the table stores or what the query does
      - 'content': full schema definition or the SQL code
    """
    client = get_qdrant_client()
    if not client:
        log.warning("Qdrant client not available; skipping metadata indexing.")
        return
        
    if not ensure_metadata_collection(client):
        return
        
    try:
        from src.ml.news_rag import embed_batch
        
        texts_to_embed = []
        structs = []
        
        for pt in points_data:
            text = f"{pt['type']} {pt['name']} {pt['description']} {pt['content']}"
            texts_to_embed.append(text)
            
        vectors = embed_batch(texts_to_embed)
        if not vectors:
            log.warning("Failed to generate embeddings for metadata points.")
            return
            
        qdrant_points = []
        for i, pt in enumerate(points_data):
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{pt['type']}:{pt['name']}"))
            qdrant_points.append(
                PointStruct(
                    id=point_id,
                    vector=vectors[i],
                    payload={
                        "type": pt["type"],
                        "name": pt["name"],
                        "description": pt["description"],
                        "content": pt["content"],
                    }
                )
            )
            
        client.upsert(collection_name=COLLECTION_NAME, points=qdrant_points)
        log.info("Indexed %d metadata points into Qdrant", len(points_data))
    except Exception as e:
        log.error("Failed to index metadata points in Qdrant: %s", e)

def retrieve_db_metadata(query_text: str, k: int = 3, type_filter: Optional[str] = None) -> list[dict[str, Any]]:
    """
    Retrieve semantically relevant ClickHouse table schemas or query templates.
    """
    client = get_qdrant_client()
    if not client:
        return []
        
    try:
        from src.ml.news_rag import embed_text
        query_vec = embed_text(query_text)
        if all(v == 0.0 for v in query_vec):
            return []
            
        if not ensure_metadata_collection(client, len(query_vec)):
            return []
            
        query_filter = None
        if type_filter:
            query_filter = Filter(must=[FieldCondition(key="type", match=MatchValue(value=type_filter))])
            
        search_result = client.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vec,
            query_filter=query_filter,
            limit=k,
            with_payload=True,
        )
        
        results = []
        for hit in search_result.points:
            payload = hit.payload or {}
            results.append({
                "type": payload.get("type", ""),
                "name": payload.get("name", ""),
                "description": payload.get("description", ""),
                "content": payload.get("content", ""),
                "score": float(hit.score),
            })
        return results
    except Exception as e:
        log.warning("Qdrant metadata retrieval failed: %s", e)
        return []
