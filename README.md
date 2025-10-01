## GleanIndexer (Python)

A lightweight Python wrapper around the Glean Indexing API to help you create datasources and index, update, and delete documents programmatically. Includes helpers for batching, bulk uploads, retry-on-rate-limit, document status, and processing triggers.

### Features
- Create or update a datasource
- Get datasource configuration
- Add allowed (beta/greenlisted) users to a datasource
- Index a single document
- Index multiple documents in batches (incremental or bulk modes)
- Get document count
- Get document status
- Delete a document
- Trigger processing for a datasource (or all datasources)
- Clear a datasource (bulk empty + process)

---

### Requirements
- Python 3.8+
- Dependencies: requests, python-dateutil

Install dependencies:
````bash
pip install requests python-dateutil
````
---

### Quickstart

Minimal example using this repository directly (module file is `GleanIndexer.py`):
````python
from GleanIndexer import GleanIndexer
indexer = GleanIndexer(api_key="YOUR_INDEXING_TOKEN", host="https://your-domain-be.glean.com/api/index/v1")
indexer.create_datasource(name="my_ds", display_name="My DS", datasource_category="CRM", url_regex=r"https://example\\.com/.*", icon_url="https://example.com/icon.png")
````

Index a single document:
````python
indexer.index_item(
  doc_id="doc-1",
  name="Sample Doc",
  datasource="my_ds",
  object_type="my_object",
  url="https://example.com/doc-1",
  description="Hello world",
  # Pass custom fields as a dict; the client converts this to Glean's customProperties list
  custom_fields={"category":"internal","priority":"p1"}
)
````

Batch index multiple documents (incremental by default):
````python
items = [
  {"id":"1","name":"Doc 1","object_type":"obj","url":"https://example.com/1","summary":"Hello 1","custom_fields":{"category":"internal"}},
  {"id":"2","name":"Doc 2","object_type":"obj","url":"https://example.com/2","summary":"Hello 2","custom_fields":{"category":"internal"}}
]
indexer.index_items(items=items, datasource="my_ds", upload_id_prefix="my_upload")
````

For bulk replacement (with optional deletion of stale docs):
````python
indexer.index_items(items=items, datasource="my_ds", upload_id_prefix="my_upload", bulk=True, deleteStale=True)
````

---

### Host and Authentication
- Host typically looks like: `https://<your-domain>-be.glean.com/api/index/v1`
- Authenticate with your Glean Indexing Token using the `Authorization: Bearer <token>` header (handled internally by `GleanIndexer`).
- SSL verification is enabled by default; you can set `verify_ssl=False` for local debugging.

---

### API Overview

- create_datasource(name, display_name, datasource_category, url_regex, icon_url, email_users=True, test_datasource=True)
  - Creates or updates a custom datasource.

- get_datasource_config(datasource_name)
  - Fetches datasource config.

- add_allowed_users(datasource_name, emails)
  - Adds allowed (beta) users to a datasource.

- index_item(doc_id, name, datasource, object_type, url, description="", tags=None, permissions=None, created_at=None, updated_at=None, custom_fields=None, max_retries=5)
  - Indexes a single document. `custom_fields` can be a dict, which is converted to the API’s `customProperties` list.

- index_items(items, datasource, upload_id_prefix, bulk=False, deleteStale=False, max_retries=5, batch_size=600, post_batch_sleep=15.0)
  - Batch index documents. If `bulk=True`, uses the bulk endpoint with paging flags and optional `deleteStale` on the last page.

- get_document_count(datasource) -> int
  - Returns the document count for the datasource.

- delete_document(doc_id, datasource, object_type, is_async=False)
  - Deletes a document.

- get_document_status(datasource, object_type, doc_id)
  - Returns processing/indexing status for a document.

- process_documents(datasource=None)
  - Triggers processing jobs. Rate-limited by Glean (commonly ~once per datasource every 3 hours).

- clear_datasource(datasource)
  - Performs an empty bulk upload to clear the datasource and triggers processing.

---

### Document Schema Notes
- Required per document: `id`, `name`, `object_type`, `url`
- Optional: `summary` (HTML), `body` (HTML), `tags` (list), `permissions` (dict), `created_at`
### Custom fields vs. customProperties
- In this client, you set custom fields using the Python argument `custom_fields` (a dict).
- Under the hood, the client converts `custom_fields` into Glean's `customProperties` list of `{name, value}` pairs.

Example conversion (conceptual):
````python
custom_fields = {"category": "internal", "priority": "p1"}
# becomes
customProperties = [
  {"name": "category", "value": "internal"},
  {"name": "priority", "value": "p1"}
]
````

- `custom_fields`: supply as a dict (`{"field": "value"}`); it is converted to the API’s `[{"name":..., "value":...}]` shape.
- Permissions default to `{"allowAnonymousAccess": True}` if not provided; set appropriate ACLs for your use case.

Custom fields example within an item:
````python
item = {
  "id":"1","name":"Doc 1","object_type":"obj","url":"https://example.com/1",
  "custom_fields": {"category": "internal", "priority": "p1"}
}
````

---

### Rate Limiting and Retries
- On HTTP 429, methods that write (e.g., `index_item`, `index_items`) inspect `Retry-After` and sleep before retrying, up to `max_retries`.
- If `Retry-After` is missing, an exception is raised to avoid hammering the API.

---

### Logging
Basic logging is configured at INFO level. You can adjust by configuring Python’s `logging` in your application.

---

### Running the included example
There is a minimal example script in this repo you can adapt.

Run it directly after filling in your credentials and values:
````bash
python example.py
````

Note: If you import directly from this repository without packaging, use `from GleanIndexer import GleanIndexer`.

---

### Troubleshooting
- 401/403: Verify your Indexing Token and tenant host. Ensure the datasource name and object types exist as expected in Glean.
- 429: You are being rate-limited; the client will back off when `Retry-After` is present. Consider reducing `batch_size` or increasing `post_batch_sleep`.
- SSL errors: Set `verify_ssl=False` for local debugging (not recommended for production).
- Validation: Ensure required keys (`id`, `name`, `object_type`, `url`) are present in every item.

---

### Disclaimer
This client is a minimal convenience wrapper for Glean’s Indexing API. You are responsible for respecting your organization’s data governance and Glean’s API limits and semantics.

