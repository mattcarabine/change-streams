import json
import time
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import re

class OperationType(str, Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"

@dataclass
class Document:
    key: str
    value: Any
    version: int
    timestamp: float
    transaction_id: int

class QueryParser:
    # Basic SQL-like operators and their Python equivalents
    OPERATORS = {
        '=': lambda x, y: x == y,
        '!=': lambda x, y: x != y,
        '>': lambda x, y: x > y,
        '>=': lambda x, y: x >= y,
        '<': lambda x, y: x < y,
        '<=': lambda x, y: x <= y,
        'IN': lambda x, y: x in y,
        'NOT IN': lambda x, y: x not in y,
        'IS NULL': lambda x, _: x is None,
        'IS NOT NULL': lambda x, _: x is not None,
        'BETWEEN': lambda x, y: y[0] <= x <= y[1]
    }

    def parse_value(self, value_str: str) -> Any:
        """Parse string value into appropriate Python type."""
        # Handle NULL
        if value_str.upper() == 'NULL':
            return None
        
        # Handle lists (for IN operator)
        if value_str.startswith('(') and value_str.endswith(')'):
            values = [v.strip().strip("'\"") for v in value_str[1:-1].split(',')]
            return values
            
        # Handle numbers
        try:
            if '.' in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            # Handle strings (remove quotes)
            return value_str.strip("'\"")

    def parse_query(self, query: str) -> tuple[str, str, Any]:
        """Parse SQL-like query into components."""
        # Basic pattern for SQL-like syntax
        patterns = [
            # BETWEEN pattern
            r"(\w+(?:\.\w+)*)\s+(BETWEEN)\s+(\d+)\s+AND\s+(\d+)",
            # IN/NOT IN pattern
            r"(\w+(?:\.\w+)*)\s+((?:NOT\s+)?IN)\s+(\([^)]+\))",
            # IS NULL/IS NOT NULL pattern
            r"(\w+(?:\.\w+)*)\s+(IS(?:\s+NOT)?\s+NULL)",
            # Basic comparison pattern
            r"(\w+(?:\.\w+)*)\s*([=!<>]+)\s*([^=!<>]+)"
        ]
        
        for pattern in patterns:
            match = re.match(pattern, query.strip(), re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 4 and groups[1].upper() == 'BETWEEN':
                    return (groups[0], 'BETWEEN', [int(groups[2]), int(groups[3])])
                field, operator, value = groups
                return (field, operator.upper(), self.parse_value(value))
                
        raise ValueError(f"Invalid query syntax: {query}")

class KeyValueStore:
    def __init__(self, storage_path: str = "kvstore.json"):
        self.storage_path = storage_path
        self.store: Dict[str, Dict[str, List[Document]]] = {}
        self.current_transaction_id = 0
        self.highest_removed_tombstone_id = 0
        self._load_from_disk()

    def _load_from_disk(self) -> None:
        """Load the store from disk if it exists."""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r') as f:
                    data = json.load(f)
                    # Filter out the last_transaction_id from the document data
                    doc_data = {
                        k: v for k, v in data.items() 
                        if k != 'last_transaction_id'
                    }
                    self.store = {
                        k: {
                            sub_k: [Document(**doc) for doc in v]
                            for sub_k, v in sub_data.items()
                        }
                        for k, sub_data in doc_data.items()
                    }
                    # Load the last transaction ID separately
                    self.current_transaction_id = data.get('last_transaction_id', 0)
            except Exception as e:
                print(f"Error loading from disk: {e}")
                self.store = {}
                self.current_transaction_id = 0

    def _save_to_disk(self) -> None:
        """Persist the store to disk."""
        try:
            with open(self.storage_path, 'w') as f:
                data = {
                    k: {
                        sub_k: [doc.__dict__ for doc in v]
                        for sub_k, v in sub_data.items()
                    }
                    for k, sub_data in self.store.items()
                }
                data['last_transaction_id'] = self.current_transaction_id
                json.dump(data, f)
        except Exception as e:
            print(f"Error saving to disk: {e}")

    def _get_next_transaction_id(self) -> int:
        """Get the next transaction ID in a thread-safe way."""
        self.current_transaction_id += 1
        return self.current_transaction_id

    def upsert(self, collection: str, key: str, value: Any) -> Document:
        """Insert or update a document in a collection."""
        if collection not in self.store:
            self.store[collection] = {}
        if key not in self.store[collection]:
            self.store[collection][key] = []
        
        version = len(self.store[collection][key]) + 1
        doc = Document(
            key=key,
            value=value,
            version=version,
            timestamp=time.time(),
            transaction_id=self._get_next_transaction_id()
        )
        self.store[collection][key].append(doc)
        self._save_to_disk()
        return doc

    def _infer_operation(self, doc: Document) -> OperationType:
        """Infer the operation type based on document state."""
        # If value is None, it's a delete (tombstone)
        if doc.value is None:
            return OperationType.DELETE
        
        # If it's version 1, it's an insert
        if doc.version == 1:
            return OperationType.INSERT
        
        # Otherwise it's an update
        return OperationType.UPDATE

    def get_changes_after(
        self, 
        transaction_id: int, 
        limit: int = 2,
        where: Optional[str] = None,
        collection: Optional[str] = None
    ) -> List[Tuple[Document, OperationType]]:
        """Get changes, optionally filtered by collection."""
        changes = []
        
        collections = [collection] if collection else self.store.keys()
        
        for col in collections:
            if col not in self.store:
                continue
            for versions in self.store[col].values():
                for doc in versions:
                    if doc.transaction_id > transaction_id:
                        if where:
                            field, operator, target = self.query_parser.parse_query(where)
                            operation = self.query_parser.OPERATORS[operator]
                            if not operation(self._get_field_value(doc, field), target):
                                continue
                        changes.append((doc, self._infer_operation(doc)))
        
        changes.sort(key=lambda x: x[0].transaction_id)
        return changes[:limit]

    def delete(self, collection: str, key: str) -> bool:
        """Delete a document from a collection."""
        if collection in self.store and key in self.store[collection]:
            # Create tombstone
            version = len(self.store[collection][key]) + 1
            tombstone = Document(
                key=key,
                value=None,
                version=version,
                timestamp=time.time(),
                transaction_id=self._get_next_transaction_id()
            )
            self.store[collection][key].append(tombstone)
            self._save_to_disk()
            return True
        return False

    def get(self, collection: str, key: str, version: Optional[int] = None) -> Optional[Document]:
        """Retrieve a document from a collection."""
        if collection not in self.store or key not in self.store[collection]:
            return None
        
        versions = self.store[collection][key]
        if not versions:
            return None
            
        if version is None:
            latest = versions[-1]
            return None if latest.value is None else latest
            
        for doc in versions:
            if doc.version == version:
                return None if doc.value is None else doc
        return None

    def garbage_collect(self, max_versions: int = 1, max_age_seconds: Optional[float] = None) -> int:
        """
        Remove old versions of documents.
        Keeps at most max_versions of each document.
        If max_age_seconds is specified, removes versions older than that.
        Returns the number of versions removed.
        """
        removed_count = 0
        current_time = time.time()

        for key in self.store:
            versions = self.store[key]
            
            # Sort by version (should already be sorted, but just to be safe)
            versions.sort(key=lambda x: x.version)
            
            removed_versions = []
            # Keep only the newest max_versions
            if len(versions) > max_versions:
                removed_versions.extend(versions[:-max_versions])
                self.store[key] = versions[-max_versions:]
            
            # Remove versions older than max_age_seconds
            if max_age_seconds is not None:
                to_keep = []
                for doc in self.store[key]:
                    if (current_time - doc.timestamp) <= max_age_seconds:
                        to_keep.append(doc)
                    else:
                        removed_versions.append(doc)
                self.store[key] = to_keep

            removed_count += len(removed_versions)
            
            # Update highest removed tombstone transaction ID
            for doc in removed_versions:
                if doc.value is None:  # This is a tombstone
                    self.highest_removed_tombstone_id = max(
                        getattr(self, 'highest_removed_tombstone_id', 0),
                        doc.transaction_id
                    )
                
        self._save_to_disk()
        return removed_count

    def list_documents(self, latest_only: bool = False) -> Dict[str, List[Document] | Document]:
        """
        List all documents in the store.
        
        Args:
            latest_only: If True, returns only the latest version of each document.
                        If False, returns all versions.
        
        Returns:
            If latest_only is True: Dict[str, Document] mapping keys to their latest versions
            If latest_only is False: Dict[str, List[Document]] mapping keys to all their versions
        """
        if latest_only:
            return {
                key: versions[-1] if versions else None
                for key, versions in self.store.items()
            }
        else:
            return self.store.copy()

    def _get_field_value(self, doc: Document, field_path: str) -> Any:
        """Get value from document using dot notation."""
        value = doc.value
        for part in field_path.split('.')[1:]:  # Skip 'value' prefix
            if not isinstance(value, dict) or part not in value:
                return None
            value = value[part]
        return value

    def query_documents(self, where_clause: str, latest_only: bool = False) -> Dict[str, List[Document] | Document]:
        """
        Query documents using SQL-like syntax.
        """
        field, operator, target = self.query_parser.parse_query(where_clause)
        operation = self.query_parser.OPERATORS[operator]
        
        results = {}
        for key, versions in self.store.items():
            matching_docs = [
                doc for doc in versions
                if operation(self._get_field_value(doc, field), target)
            ]
            
            if matching_docs:
                if latest_only:
                    results[key] = matching_docs[-1]
                else:
                    results[key] = matching_docs
        
        return results

    def evict(self, collection: str, key: str) -> bool:
        """
        Completely remove a document from the store.
        Unlike delete, this removes all history and doesn't create a tombstone.
        """
        if collection in self.store and key in self.store[collection]:
            # Get the last transaction ID before removal
            last_tx_id = self.store[collection][key][-1].transaction_id
            if last_tx_id > self.highest_removed_tombstone_id:
                self.highest_removed_tombstone_id = last_tx_id
            
            # Remove the document completely
            del self.store[collection][key]
            
            # Remove empty collections
            if not self.store[collection]:
                del self.store[collection]
                
            self._save_to_disk()
            return True
        return False