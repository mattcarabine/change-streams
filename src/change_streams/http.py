from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.openapi.utils import get_openapi
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

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
    changes: List[DocumentResponse] = Field(
        ..., 
        description="List of document changes"
    )
    max_transaction_id: int = Field(
        ..., 
        description="Current maximum transaction ID in the store"
    )

@app.put(
    "/{collection}/documents/{key}",
    response_model=DocumentResponse,
    tags=["Documents"],
    summary="Create or update a document",
    responses={
        200: {"description": "Document created or updated successfully"},
        400: {"description": "Invalid request body"},
        422: {"description": "Validation error"}
    }
)
async def upsert_document(
    collection: str = Field(..., description="Collection name"),
    key: str = Field(..., description="Document key"),
    request: Request = Field(..., description="Request with JSON body containing 'value' field")
):
    """
    Create or update a document in the specified collection.
    
    The request body must be a JSON object with a 'value' field containing any valid JSON data.
    
    Example:
    ```json
    {
        "value": {
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30
        }
    }
    ```
    """
    # ... implementation ...

@app.get(
    "/{collection}/documents/{key}",
    response_model=DocumentResponse,
    tags=["Documents"],
    summary="Get a document",
    responses={
        200: {"description": "Document retrieved successfully"},
        404: {"description": "Document not found"}
    }
)
async def get_document(
    collection: str = Field(..., description="Collection name"),
    key: str = Field(..., description="Document key"),
    version: Optional[int] = Field(None, description="Specific version to retrieve")
):
    """
    Retrieve a document by key, optionally specifying a version.
    
    If version is not specified, returns the latest version.
    """
    # ... implementation ...

@app.delete(
    "/{collection}/documents/{key}",
    tags=["Documents"],
    summary="Delete a document",
    responses={
        200: {"description": "Document deleted successfully"},
        404: {"description": "Document not found"}
    }
)
async def delete_document(
    collection: str = Field(..., description="Collection name"),
    key: str = Field(..., description="Document key")
):
    """Delete a document from the specified collection."""
    # ... implementation ...

@app.get(
    "/{collection}/documents",
    response_model=DocumentList,
    tags=["Documents"],
    summary="List documents in a collection",
    responses={
        200: {"description": "Documents retrieved successfully"},
        400: {"description": "Invalid query syntax"}
    }
)
async def list_documents(
    collection: str = Field(..., description="Collection name"),
    latest_only: bool = Field(
        False, 
        description="If true, returns only the latest version of each document"
    ),
    where: Optional[str] = Field(
        None, 
        description="SQL-like query to filter documents",
        example="value.age > 25 AND value.status = 'active'"
    )
):
    """
    List documents in a collection with optional filtering.
    
    The where parameter supports SQL-like syntax for querying JSON documents:
    - Exact match: `value.name = 'John'`
    - Numeric comparison: `value.age > 25`
    - List membership: `value.status IN ('active', 'pending')`
    - Null checks: `value.email IS NOT NULL`
    - Range checks: `value.age BETWEEN 25 AND 50`
    """
    # ... implementation ...

@app.get(
    "/changes",
    response_model=ChangesResponse,
    tags=["Changes"],
    summary="Get change feed",
    responses={
        200: {"description": "Changes retrieved successfully"},
        400: {"description": "Invalid query syntax"}
    }
)
async def get_changes(
    start: int = Field(
        0, 
        description="Return changes after this transaction ID"
    ),
    limit: int = Field(
        Query(default=2, ge=1, le=100),
        description="Maximum number of changes to return"
    ),
    where: Optional[str] = Field(
        None,
        description="SQL-like query to filter changes",
        example="value.status = 'active'"
    ),
    collection: Optional[str] = Field(
        None,
        description="Optional collection to filter changes"
    )
):
    """
    Get changes feed with optional filtering by collection and query.
    
    The response includes:
    - List of changes (documents with their operations)
    - Maximum transaction ID in the store
    
    Use the max_transaction_id to track progress and fetch next batch of changes.
    """
    # ... implementation ...

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
