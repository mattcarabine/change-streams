# Change Streams Key-Value Store

A lightweight key-value store with change streams functionality, built with FastAPI.

## Features

- Document versioning with automatic version tracking
- Change streams to monitor document modifications
- SQL-like querying capabilities
- Garbage collection of old versions
- REST API with FastAPI
- Persistent storage to disk
- Support for inserts, updates, and deletes
- Transaction ID tracking

## API Endpoints

### Documents
- `PUT /documents/{key}` - Insert or update a document
- `GET /documents/{key}` - Get latest version of a document
- `GET /documents/{key}?version={n}` - Get specific version of a document  
- `DELETE /documents/{key}` - Delete a document
- `GET /documents` - List all documents
- `GET /documents?latest_only=true` - List only latest versions
- `GET /documents?where=value.field=value` - Query documents

### Changes
- `GET /changes` - Get recent changes
- `GET /changes?start={transaction_id}` - Get changes after transaction ID
- `GET /changes?where=value.status='active'` - Filter changes by query

### Maintenance
- `POST /garbage-collect` - Clean up old versions

## Query Examples

The store supports SQL-like queries:
- Basic comparisons:
  ```sql
  value.age > 25
  value.name = 'John'
  value.active = true
  ```

- NULL checks:
  ```sql
  value.email IS NULL
  value.phone IS NOT NULL
  ```

- IN operator:
  ```sql
  value.status IN ('active', 'pending')
  value.priority IN (1, 2, 3)
  ```

- BETWEEN operator:
  ```sql
  value.age BETWEEN 25 AND 50
  value.score BETWEEN 0 AND 100
  ```

## Installation
To install the key-value store with change streams:

1. Clone the repository:

```bash
git clone https://github.com/yourusername/change-streams.git
cd change-streams
```

2. Install dependencies:

```bash
poetry install
```

3. Run the server:

```bash
poetry run uvicorn change_streams.http:app --reload
```