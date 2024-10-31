from fastapi import FastAPI, Request, Query
from fastapi.exceptions import HTTPException
from pydantic import BaseModel
from typing import Any, Optional, Dict, List
import uvicorn
import json

# Change this line to use relative import
from .store import KeyValueStore, Document  # Changed from change_streams.store

app = FastAPI(title="Key-Value Store API")
store = KeyValueStore()

# Pydantic models for request/response validation
class UpsertRequest(BaseModel):
    value: Any

class DocumentResponse(BaseModel):
    key: str
    value: Any
    version: int
    timestamp: float
    transaction_id: int

class GarbageCollectRequest(BaseModel):
    max_versions: int = 1
    max_age_seconds: Optional[float] = None

class GarbageCollectResponse(BaseModel):
    removed_count: int

# Add new response models
class DocumentList(BaseModel):
    documents: Dict[str, List[DocumentResponse] | DocumentResponse]

class ChangesResponse(BaseModel):
    changes: List[DocumentResponse]
    max_transaction_id: int

@app.put("/{collection}/documents/{key}", response_model=DocumentResponse)
async def upsert_document(collection: str, key: str, request: Request):
    """
    Insert or update a document in a collection.
    """
    try:
        body = await request.body()
        data = json.loads(body)
        
        if not isinstance(data, dict) or 'value' not in data:
            raise HTTPException(
                status_code=400,
                detail="Request body must be a JSON object with a 'value' field"
            )
        
        doc = store.upsert(collection, key, data['value'])
        return DocumentResponse(**doc.__dict__)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in request body")

@app.get("/{collection}/documents/{key}", response_model=DocumentResponse)
async def get_document(collection: str, key: str, version: Optional[int] = None):
    """Get a document from a collection by key."""
    doc = store.get(collection, key, version)
    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return DocumentResponse(**doc.__dict__)

@app.delete("/{collection}/documents/{key}")
async def delete_document(collection: str, key: str):
    """Delete a document from a collection."""
    if store.delete(collection, key):
        return {"status": "deleted"}
    raise HTTPException(status_code=404, detail="Document not found")

@app.post("/garbage-collect", response_model=GarbageCollectResponse)
async def garbage_collect(request: GarbageCollectRequest):
    """Trigger garbage collection of old document versions."""
    removed_count = store.garbage_collect(
        max_versions=request.max_versions,
        max_age_seconds=request.max_age_seconds
    )
    return GarbageCollectResponse(removed_count=removed_count)

@app.get("/{collection}/documents", response_model=DocumentList)
async def list_documents(
    collection: str,
    latest_only: bool = False,
    where: Optional[str] = None
):
    """List documents in a collection."""
    try:
        if where:
            documents = store.query_documents(collection, where, latest_only)
        else:
            documents = store.list_documents(collection, latest_only)
            
        if latest_only:
            # Convert Document objects to DocumentResponse
            response_docs = {
                key: DocumentResponse(**doc.__dict__)
                for key, doc in documents.items()
                if doc is not None  # Handle case where document might be None
            }
        else:
            # Convert list of Document objects to list of DocumentResponse
            response_docs = {
                key: [DocumentResponse(**doc.__dict__) for doc in versions]
                for key, versions in documents.items()
            }
        
        return DocumentList(documents=response_docs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/changes", response_model=ChangesResponse)
async def get_changes(
    start: int = 0,
    limit: int = Query(default=2, ge=1, le=100),
    where: Optional[str] = None,
    collection: Optional[str] = None
):
    """
    Get changes feed, optionally filtered by collection and query.
    
    Args:
        start: Return changes after this transaction ID
        limit: Maximum number of changes to return
        where: SQL-like query filter
        collection: Optional collection to filter changes
    """
    changes = store.get_changes_after(start, limit=limit, where=where, collection=collection)
    return ChangesResponse(
        changes=[
            DocumentResponse(**doc.__dict__, operation=operation.value)
            for doc, operation in changes
        ],
        max_transaction_id=store.current_transaction_id
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
