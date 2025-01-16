import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import glean_indexing_api_client as indexing_api
from glean_indexing_api_client.api import (
    datasources_api,
    documents_api,
    permissions_api,
)
from glean_indexing_api_client.model.check_document_access_request import (
    CheckDocumentAccessRequest,
)
from glean_indexing_api_client.model.check_document_access_response import (
    CheckDocumentAccessResponse,
)
from glean_indexing_api_client.model.content_definition import ContentDefinition
from glean_indexing_api_client.model.custom_datasource_config import (
    CustomDatasourceConfig,
)
from glean_indexing_api_client.model.delete_document_request import (
    DeleteDocumentRequest,
)
from glean_indexing_api_client.model.document_definition import DocumentDefinition
from glean_indexing_api_client.model.document_permissions_definition import (
    DocumentPermissionsDefinition,
)
from glean_indexing_api_client.model.get_datasource_config_request import (
    GetDatasourceConfigRequest,
)
from glean_indexing_api_client.model.get_document_count_request import (
    GetDocumentCountRequest,
)
from glean_indexing_api_client.model.get_document_count_response import (
    GetDocumentCountResponse,
)
from glean_indexing_api_client.model.get_document_status_request import (
    GetDocumentStatusRequest,
)
from glean_indexing_api_client.model.greenlist_users_request import (
    GreenlistUsersRequest,
)
from glean_indexing_api_client.model.index_document_request import IndexDocumentRequest
from glean_indexing_api_client.model.index_documents_request import (
    IndexDocumentsRequest,
)
from glean_indexing_api_client.model.user_reference_definition import (
    UserReferenceDefinition,
)
from pprint import pprint

# Configure logging (optional). Adjust the logging level as needed.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GleanIndexer:
    """
    A wrapper class for interacting with the Glean Indexing API.

    Attributes:
        api_key (str): The API key (Indexing Token) used for authentication.
        host (str): The API host URL, typically https://{your-domain}-be.glean.com/api/index/v1.
        verify_ssl (bool): Whether to verify SSL certificates.
        client (ApiClient): The configured API client instance for making requests.
    """

    def __init__(self, api_key: str, host: str, verify_ssl: bool = True):
        """
        Initialize the GleanIndexer with API credentials, host, and SSL verification preference.

        Args:
            api_key (str): The Indexing Token for authentication.
            host (str): The API host URL. Example: https://{your-domain}-be.glean.com/api/index/v1
            verify_ssl (bool): Whether to verify SSL certificates. Defaults to True.
        """
        self.api_key = api_key
        self.host = host
        self.verify_ssl = verify_ssl
        self.client = self._get_client()

    def _get_client(self) -> indexing_api.ApiClient:
        """
        Create and configure the Glean API client.

        Returns:
            ApiClient: The configured API client instance.
        """
        configuration = indexing_api.Configuration(
            host=self.host,
            access_token=self.api_key,
        )
        configuration.verify_ssl = self.verify_ssl
        return indexing_api.ApiClient(configuration)

    def create_datasource(
        self,
        name: str,
        display_name: str,
        datasource_category: str,
        url_regex: str,
        icon_url: str,
        email_users: bool = True,
        test_datasource: bool = True,
    ) -> None:
        """
        Create a new data source with the specified configuration.

        Args:
            name (str): The internal name of the data source.
            display_name (str): The human-readable name to display in the UI.
            datasource_category (str): The category of the data source (e.g., 'CRM', 'Finance').
            url_regex (str): A regex pattern to match the source's URLs for validation.
            icon_url (str): The URL of the data source's icon.
            email_users (bool): Indicates if the permissions are assigned via email addresses.
            test_datasource (bool): Indicates if this data source is for testing.

        Raises:
            Exception: If there is an error while adding the data source.
        """
        ds_api = datasources_api.DatasourcesApi(self.client)

        datasource_config = CustomDatasourceConfig(
            name=name,
            display_name=display_name,
            datasource_category=datasource_category,
            url_regex=url_regex,
            icon_url=icon_url,
            is_user_referenced_by_email=email_users,
            isTestDatasource=test_datasource,
        )

        try:
            ds_api.adddatasource_post(datasource_config)
            logger.info("Datasource '%s' successfully created.", display_name)
        except indexing_api.ApiException as e:
            logger.error("Exception creating data source: %s", e)
            raise Exception(
                f"Failed to create datasource '{display_name}' due to an API error."
            ) from e
        except Exception as general_error:
            logger.error("Unexpected error creating data source: %s", general_error)
            raise Exception(
                f"An unexpected error occurred while creating the datasource '{display_name}'."
            ) from general_error

    def get_datasource_config(self, datasource_name: str) -> Any:
        """
        Retrieve the configuration of a specified data source.

        Args:
            datasource_name (str): The name of the data source to retrieve configuration for.

        Raises:
            Exception: If there is an error while fetching the data source configuration.

        Returns:
            Any: The data source configuration retrieved from the API.
        """
        ds_api = datasources_api.DatasourcesApi(self.client)
        request = GetDatasourceConfigRequest(datasource=datasource_name)

        try:
            api_response = ds_api.getdatasourceconfig_post(request)
            pprint(
                api_response
            )  # For readability; consider removing/using logs for production
            return api_response
        except indexing_api.ApiException as e:
            logger.error("Exception fetching data source config: %s", e)
            raise Exception(
                f"Failed to retrieve the configuration for datasource '{datasource_name}' due to an API error."
            ) from e
        except Exception as general_error:
            logger.error(
                "Unexpected error fetching data source config: %s", general_error
            )
            raise Exception(
                f"An unexpected error occurred while retrieving the configuration for datasource '{datasource_name}'."
            ) from general_error

    def add_allowed_users(self, datasource_name: str, emails: List[str]) -> None:
        """
        Add a list of users to the allowed (greenlist) users for a specified data source.

        Args:
            datasource_name (str): The name of the data source to which users should be allowed.
            emails (List[str]): A list of email addresses to allow.

        Raises:
            Exception: If there is an error while adding the allowed users.
            ValueError: If no email addresses are provided.
        """
        if not emails:
            raise ValueError("At least one email address must be provided.")

        perm_api = permissions_api.PermissionsApi(self.client)
        greenlist_users_request = GreenlistUsersRequest(
            datasource=datasource_name, emails=emails
        )

        try:
            perm_api.betausers_post(greenlist_users_request)
            logger.info(
                "Successfully allowed users %s for datasource '%s'.",
                emails,
                datasource_name,
            )
        except indexing_api.ApiException as e:
            logger.error("Exception adding allowed users: %s", e)
            raise Exception(
                f"Failed to allow users for datasource '{datasource_name}' due to an API error."
            ) from e
        except Exception as general_error:
            logger.error("Unexpected error adding allowed users: %s", general_error)
            raise Exception(
                f"An unexpected error occurred while allowing users for datasource '{datasource_name}'."
            ) from general_error

    def index_item(
        self,
        doc_id: str,
        name: str,
        datasource: str,
        object_type: str,
        url: str,
        description: str = "",
        tags: Optional[List[str]] = None,
        permissions: Optional[DocumentPermissionsDefinition] = None,
        created_at: Optional[int] = None,
        max_retries: int = 5,
        **custom_fields: Any,
    ) -> Any:
        """
        Index a single item (document) to a specified data source with customizable fields.
        Automatically retries if rate-limited.

        Args:
            doc_id (str): Unique document ID.
            name (str): Title of the document.
            datasource (str): Data source name.
            object_type (str): The object type for this document.
            url (str): The URL to view the document.
            description (str, optional): A text description of the document. Defaults to "".
            tags (List[str], optional): A list of tags. Defaults to None.
            permissions (DocumentPermissionsDefinition, optional): Document permissions. Defaults to None.
            created_at (int, optional): Epoch timestamp for creation date. Defaults to now.
            max_retries (int, optional): Max number of retries if rate-limited. Defaults to 5.
            **custom_fields: Additional custom fields to be included in the document.
                            Ensure these fields are first set up in the Glean UI.

        Returns:
            Any: The response object after indexing the document.

        Raises:
            Exception: If an error occurs or if rate limits are exceeded after max retries.
        """
        doc_api = documents_api.DocumentsApi(self.client)
        tags = tags or []

        # Default permissions if not provided
        if not permissions:
            permissions = DocumentPermissionsDefinition(allow_anonymous_access=True)

        # Default creation timestamp if not provided
        if not created_at:
            created_at = int(datetime.now().timestamp())

        current_time_epoch = int(datetime.now().timestamp())

        # Prepare custom properties
        custom_properties = [
            {"name": key, "value": value if value is not None else ""}
            for key, value in custom_fields.items()
        ]

        request = IndexDocumentRequest(
            document=DocumentDefinition(
                datasource=datasource,
                object_type=object_type,
                title=name,
                id=doc_id,
                created_at=created_at,
                updated_at=current_time_epoch,
                tags=tags + [datasource],  # Add datasource name as a tag
                view_url=url,
                summary=ContentDefinition(
                    mime_type="text/html", text_content=description
                ),
                custom_properties=custom_properties,
                permissions=permissions,
            )
        )

        retries = 0
        while retries <= max_retries:
            try:
                response = doc_api.indexdocument_post(request)
                logger.info("Document '%s' successfully indexed.", doc_id)
                return response
            except indexing_api.ApiException as e:
                if e.status == 429:
                    # Handle rate limiting
                    retry_after = e.headers.get("Retry-After")
                    if retry_after is not None:
                        wait_time = int(retry_after)
                        logger.warning(
                            "Rate limit exceeded. Retrying after %s seconds...",
                            wait_time,
                        )
                        time.sleep(wait_time)
                        retries += 1
                        continue
                    else:
                        logger.error(
                            "Rate limit exceeded but 'Retry-After' header is missing."
                        )
                        raise Exception(
                            f"Failed to index document '{doc_id}' due to rate limiting."
                        ) from e
                else:
                    logger.error("Exception indexing document: %s", e)
                    raise Exception(
                        f"Failed to index document '{doc_id}' due to an API error."
                    ) from e

        raise Exception(
            f"Failed to index document '{doc_id}' after {max_retries} retries due to rate limiting."
        )

    def index_items(
        self,
        items: List[Dict[str, Any]],
        datasource: str,
        upload_id_prefix: str,
        max_retries: int = 5,
        batch_size: int = 600,
        post_batch_sleep: float = 15.0,
    ) -> None:
        """
        Index multiple items (documents) to a specified data source in batches.

        Args:
            items (List[Dict[str, Any]]): A list of item dicts. Each dict must have:
                'id', 'name', 'object_type', 'url' as required fields.
                Optional: 'description', 'tags', 'permissions', 'created_at', 'custom_fields'.
            datasource (str): The data source name.
            upload_id_prefix (str): Prefix used to generate a unique upload ID per batch.
            max_retries (int, optional): Maximum number of retries if rate-limited. Defaults to 5.
            batch_size (int, optional): Number of documents per batch. Defaults to 600.
            post_batch_sleep (float, optional): Sleep time (in seconds) after a successful batch
                                                to avoid rate limits. Defaults to 15.0.

        Raises:
            ValueError: If an item lacks the required fields.
            Exception: If an API error or rate limit error persists after max retries.
        """
        doc_api = documents_api.DocumentsApi(self.client)

        def chunked(iterable: List[Dict[str, Any]], size: int):
            """Yield successive n-sized chunks from an iterable."""
            for i in range(0, len(iterable), size):
                yield iterable[i : i + size]

        batch_num = 0

        for batch in chunked(items, batch_size):
            batch_num += 1
            unique_id = uuid.uuid4()
            upload_id = f"{upload_id_prefix}_batch_{batch_num}_{unique_id}"

            documents = []
            current_time_epoch = int(datetime.now().timestamp())

            for item in batch:
                doc_id = item.get("id")
                name = item.get("name")
                object_type = item.get("object_type")
                url = item.get("url")

                if not all([doc_id, name, object_type, url]):
                    raise ValueError(
                        "Each item must have 'id', 'name', 'object_type', and 'url'."
                    )

                description = item.get("description", "")
                tags = item.get("tags", [])
                permissions = item.get("permissions") or DocumentPermissionsDefinition(
                    allow_anonymous_access=True
                )
                created_at = item.get("created_at") or current_time_epoch
                custom_fields = item.get("custom_fields", {})

                # Prepare custom properties
                custom_props = [
                    {"name": key, "value": value if value is not None else ""}
                    for key, value in custom_fields.items()
                ]

                document = DocumentDefinition(
                    datasource=datasource,
                    object_type=object_type,
                    title=name,
                    id=doc_id,
                    created_at=created_at,
                    updated_at=current_time_epoch,
                    tags=tags + [datasource],
                    view_url=url,
                    summary=ContentDefinition(
                        mime_type="text/html", textContent=description
                    ),
                    custom_properties=custom_props,
                    permissions=permissions,
                )
                documents.append(document)

            request = IndexDocumentsRequest(
                upload_id=upload_id, datasource=datasource, documents=documents
            )

            retries = 0
            while retries <= max_retries:
                try:
                    doc_api.indexdocuments_post(request)
                    logger.info("Batch %d: Documents successfully indexed.", batch_num)
                    # Sleep after successful batch to reduce rate-limit risk
                    time.sleep(post_batch_sleep)
                    break
                except indexing_api.ApiException as e:
                    if e.status == 429:
                        retry_after = e.headers.get("Retry-After")
                        if retry_after is not None:
                            wait_time = int(retry_after)
                            logger.warning(
                                "Batch %d: Rate limit exceeded. Retrying in %s seconds...",
                                batch_num,
                                wait_time,
                            )
                            time.sleep(wait_time)
                            retries += 1
                            continue
                        else:
                            logger.error(
                                "Batch %d: Rate limit exceeded but 'Retry-After' header is missing.",
                                batch_num,
                            )
                            raise Exception(
                                f"Failed to index documents in batch {batch_num} due to rate limiting."
                            ) from e
                    else:
                        logger.error("Exception indexing batch %d: %s", batch_num, e)
                        raise Exception(
                            f"Failed to index documents in batch {batch_num} due to an API error."
                        ) from e
            else:
                # Max retries exceeded
                raise Exception(
                    f"Failed to index documents in batch {batch_num} after {max_retries} retries due to rate limiting."
                )

    def get_document_count(self, datasource_name: str) -> GetDocumentCountResponse:
        """
        Retrieve the document count for a specified data source.

        Args:
            datasource_name (str): The name of the data source.

        Raises:
            Exception: If there is an error while fetching the document count.

        Returns:
            GetDocumentCountResponse: The document count data retrieved from the API.
        """
        doc_api = documents_api.DocumentsApi(self.client)
        request = GetDocumentCountRequest(datasource=datasource_name)

        try:
            response = doc_api.getdocumentcount_post(request)
            pprint(response)
            return response
        except indexing_api.ApiException as e:
            logger.error("Exception fetching document count: %s", e)
            raise Exception(
                f"Failed to retrieve the document count for datasource '{datasource_name}' due to an API error."
            ) from e
        except Exception as general_error:
            logger.error("Unexpected error fetching document count: %s", general_error)
            raise Exception(
                f"An unexpected error occurred while retrieving the document count for datasource '{datasource_name}'."
            ) from general_error

    def delete_document(
        self,
        doc_id: str,
        datasource: str,
        object_type: str,
        is_async: bool = False,
    ) -> None:
        """
        Delete a document from the specified data source.

        Args:
            doc_id (str): The unique document ID.
            datasource (str): The data source name.
            object_type (str): The type of object.
            is_async (bool): Whether to perform the delete call asynchronously. Defaults to False.

        Raises:
            Exception: If an error occurs during deletion.
        """
        doc_api = documents_api.DocumentsApi(self.client)
        request = DeleteDocumentRequest(
            datasource=datasource, object_type=object_type, id=doc_id
        )

        try:
            doc_api.deletedocument_post(request, async_req=is_async)
            logger.info(
                "Document '%s' deletion initiated (async=%s).", doc_id, is_async
            )
        except Exception as e:
            logger.error("Error deleting document '%s': %s", doc_id, e)
            raise Exception(f"Failed to delete document '{doc_id}'.") from e

    def get_document_status(
        self, datasource_name: str, object_type: str, doc_id: str
    ) -> Any:
        """
        Retrieve the status of a specific document in a data source.

        Args:
            datasource_name (str): The name of the data source (e.g., "planhat").
            object_type (str): The type of object (e.g., "CRM").
            doc_id (str): The unique document ID.

        Raises:
            Exception: If there is an error while fetching the document status.

        Returns:
            Any: The document status data retrieved from the API.
        """
        doc_api = documents_api.DocumentsApi(self.client)
        request = GetDocumentStatusRequest(
            datasource=datasource_name, object_type=object_type, doc_id=doc_id
        )

        try:
            response = doc_api.getdocumentstatus_post(request)
            pprint(response)
            return response
        except indexing_api.ApiException as e:
            logger.error("Exception fetching document status: %s", e)
            raise Exception(
                f"Failed to retrieve the status of document '{doc_id}' from datasource '{datasource_name}'."
            ) from e
        except Exception as general_error:
            logger.error("Unexpected error fetching document status: %s", general_error)
            raise Exception(
                f"An unexpected error occurred while retrieving the status of document '{doc_id}' "
                f"from datasource '{datasource_name}'."
            ) from general_error
