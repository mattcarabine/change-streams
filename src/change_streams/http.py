from fastapi import FastAPI, Query, HTTPException, Path, Body, status
from fastapi.openapi.utils import get_openapi
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

from .store import KeyValueStore

# Initialize the store
store = KeyValueStore()

app = FastAPI(
    title="Change Streams API",
    description="""
    A document store with versioning and change feed capabilities.
    
    ## Features
    - Document versioning
    - Change feed with transaction IDs
    - SQL-like query language
    - Collection-based namespacing
    
    ## Query Language Examples
    - Exact match: `value.name = 'John'`
    - Numeric comparison: `value.age > 25`
    - List membership: `value.status IN ('active', 'pending')`
    - Null checks: `value.email IS NOT NULL`
    - Range checks: `value.age BETWEEN 25 AND 50`
    """,
    version="1.0.0",
    contact={
        "name": "API Support",
        "email": "support@example.com",
    },
)

class OperationType(str, Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"

class DocumentResponse(BaseModel):
    key: str = Field(..., description="Unique identifier of the document within its collection")
    value: Any = Field(..., description="Document content (any valid JSON)")
    version: int = Field(..., description="Document version number")
    timestamp: float = Field(..., description="Unix timestamp of the operation")
    transaction_id: int = Field(..., description="Globally unique, monotonic transaction ID")
    operation: Optional[OperationType] = Field(None, description="Type of operation (in change feed)")

    class Config:
        schema_extra = {
            "example": {
                "key": "user:1",
                "value": {
                    "name": "John Doe",
                    "email": "john@example.com",
                    "age": 30
                },
                "version": 1,
                "timestamp": 1234567890.123,
                "transaction_id": 42,
                "operation": "insert"
            }
        }

class DocumentList(BaseModel):
    documents: Dict[str, List[DocumentResponse] | DocumentResponse] = Field(
        ..., 
        description="Map of document keys to their versions or latest version"
    )

class ChangesResponse(BaseModel):
    changes: List[DocumentResponse]
    max_transaction_id: int
    needs_rollback: bool = Field(
        False,
        description="If true, the client needs to rollback and reload their data"
    )

class DocumentInput(BaseModel):
    value: Any = Field(..., description="Document content (any valid JSON)")

    class Config:
        schema_extra = {
            "example": {
                "value": {
                    "name": "John Doe",
                    "email": "john@example.com",
                    "age": 30
                }
            }
        }

@app.put(
    "/{collection}/documents/{key}",
    response_model=DocumentResponse,
    tags=["Documents"],
    summary="Create or update a document"
)
async def upsert_document(
    document: DocumentInput = Body(..., description="Document to create or update"),
    collection: str = Path(..., description="Collection name"),
    key: str = Path(..., description="Document key")
):
    """Create or update a document in the specified collection."""
    try:
        doc = store.upsert(collection, key, document.value)
        return DocumentResponse(**doc.__dict__)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get(
    "/{collection}/documents/{key}",
    response_model=DocumentResponse,
    tags=["Documents"],
    summary="Get a document"
)
async def get_document(
    collection: str = Path(..., description="Collection name"),
    key: str = Path(..., description="Document key"),
    version: Optional[int] = Query(None, description="Specific version to retrieve")
):
    """Retrieve a document by key, optionally specifying a version."""
    doc = store.get(collection, key, version)
    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return DocumentResponse(**doc.__dict__)

@app.delete(
    "/{collection}/documents/{key}",
    tags=["Documents"],
    summary="Delete a document"
)
async def delete_document(
    collection: str = Path(..., description="Collection name"),
    key: str = Path(..., description="Document key")
):
    """Delete a document from the specified collection."""
    if store.delete(collection, key):
        return {"status": "deleted"}
    raise HTTPException(status_code=404, detail="Document not found")

@app.get(
    "/{collection}/documents",
    response_model=DocumentList,
    tags=["Documents"],
    summary="List documents in a collection"
)
async def list_documents(
    collection: str = Path(..., description="Collection name"),
    latest_only: bool = Query(False, description="If true, returns only the latest version of each document"),
    where: Optional[str] = Query(
        None, 
        description="SQL-like query to filter documents",
        example="value.age > 25 AND value.status = 'active'"
    )
):
    """List documents in a collection with optional filtering."""
    try:
        if where:
            documents = store.query_documents(collection, where, latest_only)
        else:
            documents = store.list_documents(collection, latest_only)
        return DocumentList(documents=documents)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get(
    "/changes",
    response_model=ChangesResponse,
    tags=["Changes"],
    summary="Get change feed",
    responses={
        200: {
            "description": "Changes retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "changes": [
                            {
                                "key": "user:1",
                                "value": {"name": "John"},
                                "version": 1,
                                "timestamp": 1234567890.123,
                                "transaction_id": 42,
                                "operation": "insert"
                            }
                        ],
                        "max_transaction_id": 42,
                        "needs_rollback": False
                    }
                }
            }
        },
        409: {
            "description": "Client needs to rollback and reload data",
            "content": {
                "application/json": {
                    "example": {
                        "changes": [],
                        "max_transaction_id": 42,
                        "needs_rollback": True
                    }
                }
            }
        }
    }
)
async def get_changes(
    start: int = Query(0, description="Return changes after this transaction ID"),
    limit: int = Query(2, ge=1, le=100, description="Maximum number of changes to return"),
    where: Optional[str] = Query(
        None,
        description="SQL-like query to filter changes",
        example="value.status = 'active'"
    ),
    collection: Optional[str] = Query(None, description="Optional collection to filter changes")
):
    """
    Get changes feed with optional filtering by collection and query.
    
    If the client's start transaction ID is older than the oldest available
    tombstone, a rollback response will be returned indicating that the
    client needs to reload their data.
    """
    # Check if client needs to rollback
    if start < store.highest_removed_tombstone_id:
        return ChangesResponse(
            changes=[],
            max_transaction_id=store.current_transaction_id,
            needs_rollback=True
        )

    changes = store.get_changes_after(start, limit=limit, where=where, collection=collection)
    return ChangesResponse(
        changes=[
            DocumentResponse(**doc.__dict__, operation=operation.value)
            for doc, operation in changes
        ],
        max_transaction_id=store.current_transaction_id,
        needs_rollback=False
    )

@app.post(
    "/{collection}/documents/{key}/evict",
    tags=["Documents"],
    summary="Evict a document",
    responses={
        200: {"description": "Document evicted successfully"},
        404: {"description": "Document not found"}
    }
)
async def evict_document(
    collection: str = Path(..., description="Collection name"),
    key: str = Path(..., description="Document key")
):
    """
    Completely remove a document from the store.
    
    Unlike delete, eviction:
    - Removes all document history
    - Doesn't create a tombstone
    - Cannot be tracked via changes feed
    - Affects change feed rollback behavior
    
    Warning: Use with caution as this operation is irreversible.
    """
    if store.evict(collection, key):
        return {
            "status": "evicted",
            "warning": "Document and all its history have been permanently removed"
        }
    raise HTTPException(status_code=404, detail="Document not found")

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="Change Streams API",
        version="1.0.0",
        description=app.description,
        routes=app.routes,
    )
    
    # Add security schemes if needed
    # openapi_schema["components"]["securitySchemes"] = {...}
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi
