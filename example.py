"""
Example usage script for interacting with the Glean Indexing API via the GleanIndexer class.
"""

import logging
from GleanIndexer import GleanIndexer  # Import from this repo's module file

# Optional: Configure logging here or in glean_indexer.py
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # 1. Initialize the indexer
    glean_indexer = GleanIndexer(
        api_key="YOUR_API_KEY",  # Replace with your actual Indexing Token
        host="https://YOUR_GLEAN_HOST/api/index/v1",  # Replace with your actual host
        verify_ssl=True  # Change to False if you need to skip SSL verification
    )

    # 2. Create a new datasource
    #    Provide meaningful values and ensure they don't clash with existing datasources in your environment.
    try:
        glean_indexer.create_datasource(
            name="my_datasource_internal_name",
            display_name="My Data Source",
            datasource_category="CRM",
            url_regex="https://example\\.com/.*",
            icon_url="https://example.com/icon.png",
            email_users=True,
            test_datasource=True
        )
    except Exception as e:
        logger.error("Error creating data source: %s", e)

    # 3. Retrieve data source configuration
    try:
        config_response = glean_indexer.get_datasource_config("my_datasource_internal_name")
        logger.info("Data source config: %s", config_response)
    except Exception as e:
        logger.error("Error fetching data source config: %s", e)

    # 4. Add allowed (greenlisted) users
    try:
        glean_indexer.add_allowed_users(
            datasource_name="my_datasource_internal_name",
            emails=["user1@example.com", "user2@example.com"]
        )
    except Exception as e:
        logger.error("Error adding allowed users: %s", e)

    # 5. Index a single document
    try:
        index_response = glean_indexer.index_item(
            doc_id="unique_doc_id_123",
            name="Sample Document",
            datasource="my_datasource_internal_name",
            object_type="my_object_type",
            url="https://example.com/my_doc",
            description="A sample description for the doc.",  # becomes document.summary
            tags=["tag1", "tag2"],
            # Provide custom fields as a dict; they will be converted to customProperties
            custom_fields={
                "custom_field1": "Custom Value 1",
                "custom_field2": "Custom Value 2"
            }
        )
        logger.info("Index single doc response: %s", index_response)
    except Exception as e:
        logger.error("Error indexing single document: %s", e)

    # 6. Index multiple documents in batches
    #    Build your items list as needed. Each item should have at least
    #    'id', 'name', 'object_type', 'url' keys.
    items_to_index = [
        {
            "id": "doc_id_1",
            "name": "Multiple Doc 1",
            "object_type": "object_type_1",
            "url": "https://example.com/doc1",
            "summary": "Summary for doc 1",
            "tags": ["batch", "example"],
            "custom_fields": {
                "my_custom_field": "Custom Value 1"
            }
        },
        {
            "id": "doc_id_2",
            "name": "Multiple Doc 2",
            "object_type": "object_type_1",
            "url": "https://example.com/doc2",
            "summary": "Summary for doc 2",
            "tags": ["batch", "example2"],
            "custom_fields": {
                "another_custom_field": "Custom Value 2"
            }
        }
        # ...more items as needed...
    ]
    try:
        glean_indexer.index_items(
            items=items_to_index,
            datasource="my_datasource_internal_name",
            upload_id_prefix="my_batch_upload",
            max_retries=5,
            batch_size=2,         # for demo purposes, batch_size is small
            post_batch_sleep=5.0  # reduce or increase as needed
        )
    except Exception as e:
        logger.error("Error indexing multiple documents: %s", e)

    # 7. Retrieve the document count for the data source
    try:
        doc_count_resp = glean_indexer.get_document_count("my_datasource_internal_name")
        logger.info("Document count response: %s", doc_count_resp)
    except Exception as e:
        logger.error("Error fetching document count: %s", e)

    # 8. Check the status of a document
    try:
        doc_status_resp = glean_indexer.get_document_status(
            datasource="my_datasource_internal_name",
            object_type="my_object_type",
            doc_id="unique_doc_id_123"
        )
        logger.info("Document status response: %s", doc_status_resp)
    except Exception as e:
        logger.error("Error fetching document status: %s", e)

    # 9. Delete a document
    try:
        glean_indexer.delete_document(
            doc_id="unique_doc_id_123",
            datasource="my_datasource_internal_name",
            object_type="my_object_type",
            is_async=False  # set True to make it an async call
        )
        logger.info("Document 'unique_doc_id_123' deletion initiated.")
    except Exception as e:
        logger.error("Error deleting document: %s", e)


if __name__ == "__main__":
    main()
