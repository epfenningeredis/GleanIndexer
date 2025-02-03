import requests
import time
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Union, Optional
from dateutil import parser as date_parser

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GleanIndexer:
    """
    A wrapper class for interacting with the Glean Indexing API using requests.

    Attributes:
        api_key (str): The API key (Indexing Token) used for authentication.
        host (str): The API host URL, typically "https://{your-domain}-be.glean.com/api/index/v1".
        verify_ssl (bool): Whether SSL certificates should be verified.
        headers (Dict[str, str]): Default headers used in API requests.
    """

    def __init__(self, api_key: str, host: str, verify_ssl: bool = True):
        """
        Initialize the GleanIndexer with API credentials, host URL, and SSL verification preference.

        Args:
            api_key (str): The Indexing Token for authentication.
            host (str): The API host URL (e.g., "https://{your-domain}-be.glean.com/api/index/v1").
            verify_ssl (bool, optional): Whether to verify SSL certificates. Defaults to True.
        """
        self.api_key = api_key
        self.host = host.rstrip("/")  # Remove trailing slash if present
        self.verify_ssl = verify_ssl

        # Set common headers for all API requests.
        self.headers: Dict[str, str] = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _handle_response(self, response: requests.Response) -> Any:
        """
        Process the HTTP response.

        - Returns the JSON (or text) if the request is successful.
        - Returns the raw response for a 429 status (so the caller can handle retries).
        - Raises an exception for other non-200 statuses.

        Args:
            response (requests.Response): The HTTP response object.

        Returns:
            Any: The parsed JSON response or raw text.
        """
        if response.status_code == 429:
            return response  # Caller handles rate-limit retries

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.error("HTTP error occurred: %s", e)
            raise Exception(f"Request failed with status {response.status_code}: {response.text}") from e

        try:
            return response.json()
        except ValueError:
            return response.text

    @staticmethod
    def to_epoch(value: Optional[Union[int, str]]) -> int:
        """
        Convert a value to epoch seconds.

        Args:
            value (Optional[Union[int, str]]): An integer (epoch seconds), a date string, or None.

        Returns:
            int: The value converted to epoch seconds. If None, returns the current time.
        """
        if value is None:
            return int(datetime.now().timestamp())
        if isinstance(value, int):
            return value
        elif isinstance(value, str):
            dt = date_parser.parse(value)
            return int(dt.timestamp())
        else:
            raise ValueError("Timestamp must be either an integer (epoch) or a valid date string.")

    def _convert_custom_fields(self, custom_fields: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert custom fields provided as a dictionary into a list of dictionaries
        with 'name' and 'value' keys as expected by the API.

        Args:
            custom_fields (Optional[Dict[str, Any]]): Custom fields as a dictionary.

        Returns:
            List[Dict[str, Any]]: Custom fields in list format.
        """
        if not custom_fields:
            return []
        return [{"name": key, "value": str(value)} for key, value in custom_fields.items()]

    # -------------------------------------------------------------------
    # 1) Create or Update a Data Source
    # -------------------------------------------------------------------
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
        Create or update a custom datasource.

        Args:
            name (str): Internal name of the datasource.
            display_name (str): Human-readable name for display.
            datasource_category (str): Category (e.g., "CRM", "Finance").
            url_regex (str): Regex pattern for validating URLs.
            icon_url (str): URL for the datasource icon.
            email_users (bool, optional): Whether the datasource uses email-based user referencing.
            test_datasource (bool, optional): Whether the datasource is for testing purposes.
        """
        endpoint = f"{self.host}/adddatasource"
        body = {
            "name": name,
            "displayName": display_name,
            "datasourceCategory": datasource_category,
            "urlRegex": url_regex,
            "iconUrl": icon_url,
            "isUserReferencedByEmail": email_users,
            "isTestDatasource": test_datasource,
        }

        logger.info("Creating/updating datasource '%s'...", display_name)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        data = self._handle_response(resp)
        if resp.status_code == 200:
            logger.info("Datasource '%s' created/updated successfully.", display_name)
        else:
            raise Exception(f"Failed to create/update datasource '{display_name}'. Response: {data}")

    # -------------------------------------------------------------------
    # 2) Retrieve Datasource Configuration
    # -------------------------------------------------------------------
    def get_datasource_config(self, datasource_name: str) -> Any:
        """
        Retrieve the configuration for a specified datasource.

        Args:
            datasource_name (str): Name of the datasource.

        Returns:
            Any: Datasource configuration as returned by the API.
        """
        endpoint = f"{self.host}/getdatasourceconfig"
        body = {"datasource": datasource_name}

        logger.info("Retrieving config for datasource '%s'...", datasource_name)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        return self._handle_response(resp)

    # -------------------------------------------------------------------
    # 3) Add Allowed (Beta) Users to a Datasource
    # -------------------------------------------------------------------
    def add_allowed_users(self, datasource_name: str, emails: List[str]) -> None:
        """
        Add allowed (beta) users to a datasource.

        Args:
            datasource_name (str): Name of the datasource.
            emails (List[str]): List of email addresses to allow.

        Raises:
            ValueError: If the emails list is empty.
        """
        if not emails:
            raise ValueError("At least one email address must be provided.")

        endpoint = f"{self.host}/betausers"
        body = {"datasource": datasource_name, "emails": emails}

        logger.info("Adding allowed users to datasource '%s': %s", datasource_name, emails)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        data = self._handle_response(resp)
        if resp.status_code == 200:
            logger.info("Successfully allowed users for datasource '%s'.", datasource_name)
        else:
            raise Exception(f"Failed to add allowed users. Response: {data}")

    # -------------------------------------------------------------------
    # 4) Index a Single Document
    # -------------------------------------------------------------------
    def index_item(
        self,
        doc_id: str,
        name: str,
        datasource: str,
        object_type: str,
        url: str,
        description: str = "",
        tags: Optional[List[str]] = None,
        permissions: Optional[Dict[str, Any]] = None,
        created_at: Optional[Union[int, str]] = None,
        updated_at: Optional[Union[int, str]] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
        max_retries: int = 5,
    ) -> Any:
        """
        Index a single document.

        This method sends a document to the /indexdocument endpoint. The payload is built to conform
        to the API schema. Custom properties are sent using the "customProperties" key as a list.

        Args:
            doc_id (str): Unique document ID.
            name (str): Document title.
            datasource (str): Datasource name.
            object_type (str): Object type of the document.
            url (str): URL for viewing the document.
            description (str): Document description.
            tags (Optional[List[str]]): List of tags.
            permissions (Optional[Dict[str, Any]]): Permissions JSON.
            created_at (Optional[Union[int, str]]): Creation time (epoch int or date string).
            updated_at (Optional[Union[int, str]]): Update time (epoch int or date string).
            custom_fields (Optional[Dict[str, Any]]): Custom properties as a dictionary (e.g.,
                {"languages": f"{book['languages']}", "bookshelves": f"{book['bookshelves']}"})
            max_retries (int): Maximum number of retries on rate-limiting.

        Returns:
            Any: Response from the Glean API.
        """
        endpoint = f"{self.host}/indexdocument"
        tags = tags or []
        if not permissions:
            permissions = {"allowAnonymousAccess": True}

        created_at_epoch = self.to_epoch(created_at)
        updated_at_epoch = self.to_epoch(updated_at)

        # Convert custom_fields dictionary to a list of {name, value} objects.
        custom_properties = self._convert_custom_fields(custom_fields)

        payload = {
            "document": {
                "title": name,
                "datasource": datasource,
                "objectType": object_type,
                "viewURL": url,
                "id": doc_id,
                "summary": {"mimeType": "text/html", "textContent": description},
                "createdAt": created_at_epoch,
                "updatedAt": updated_at_epoch,
                "tags": tags + [datasource],
                "customProperties": custom_properties,
                "permissions": permissions,
            }
        }

        retries = 0
        while retries <= max_retries:
            logger.info("Indexing document '%s' (attempt %d/%d)...", doc_id, retries + 1, max_retries)
            resp = requests.post(endpoint, headers=self.headers, json=payload, verify=self.verify_ssl)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait_time = int(retry_after)
                    logger.warning("Rate limit exceeded. Retrying after %s seconds...", wait_time)
                    time.sleep(wait_time)
                    retries += 1
                    continue
                else:
                    raise Exception(f"429 Too Many Requests but no 'Retry-After' header for doc '{doc_id}'.")
            data = self._handle_response(resp)
            logger.info("Document '%s' successfully indexed.", doc_id)
            return data

        raise Exception(f"Failed to index document '{doc_id}' after {max_retries} retries.")

    # -------------------------------------------------------------------
    # 5) Index Multiple Documents (Bulk/Incremental)
    # -------------------------------------------------------------------
    def index_items(
        self,
        items: List[Dict[str, Any]],
        datasource: str,
        upload_id_prefix: str,
        bulk: bool = False,
        deleteStale: bool = False,
        max_retries: int = 5,
        batch_size: int = 600,
        post_batch_sleep: float = 15.0,
    ) -> None:
        """
        Index multiple documents in batches.

        Each item in 'items' should be a dictionary with the following keys:
            - id (str): Required.
            - name (str): Required.
            - object_type (str): Required.
            - url (str): Required.
            - summary (str): Optional.
            - body (str): Optional.
            - tags (List[str]): Optional.
            - permissions (dict): Optional.
            - created_at (Union[int, str]): Optional; epoch or date string.
            - custom_fields (dict): Optional; custom properties as a dictionary.
        
        Args:
            items (List[Dict[str, Any]]): List of document items to index.
            datasource (str): Datasource name.
            upload_id_prefix (str): Prefix for the unique uploadId.
            bulk (bool): If True, use the /bulkindexdocuments endpoint (which replaces documents)
                        and include isFirstPage, isLastPage, and forceRestartUpload flags.
                        If False (default), use /indexdocuments (incremental indexing).
            deleteStale (bool) If True, and bulk upload is used, will delete all documents not a part of the bulk upload.
            max_retries (int): Maximum number of retries on rate-limiting.
            batch_size (int): Number of documents per batch.
            post_batch_sleep (float): Sleep time (in seconds) after each batch.
        """
        # Choose endpoint based on bulk flag.
        endpoint = f"{self.host}/bulkindexdocuments" if bulk else f"{self.host}/indexdocuments"

        def chunked(iterable: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
            """Split the list into chunks of the specified size."""
            return [iterable[i : i + size] for i in range(0, len(iterable), size)]

        batches = chunked(items, batch_size)
        total_batches = len(batches)

        for batch_num, batch in enumerate(batches, start=1):
            unique_id = uuid.uuid4()
            upload_id = f"{upload_id_prefix}_batch_{batch_num}_{unique_id}"

            documents = []
            current_time_epoch = int(datetime.now().timestamp())

            for item in batch:
                # Validate required keys.
                for key in ["id", "name", "object_type", "url"]:
                    if key not in item:
                        raise ValueError(f"Each item must have '{key}'.")

                doc_id = item["id"]
                name = item["name"]
                object_type = item["object_type"]
                url = item["url"]
                summary = item.get("summary", "")
                body_text = item.get("body", "")
                tags = item.get("tags", [])
                permissions = item.get("permissions") or {"allowAnonymousAccess": True}
                created_at_epoch = self.to_epoch(item.get("created_at"))
                updated_at_epoch = current_time_epoch
                custom_fields = item.get("custom_fields", {})

                # Convert custom_fields from dict to list of {name, value} objects.
                custom_properties = self._convert_custom_fields(custom_fields)

                document = {
                    "title": name,
                    "datasource": datasource,
                    "objectType": object_type,
                    "viewURL": url,
                    "id": doc_id,
                    "summary": {"mimeType": "text/html", "textContent": summary},
                    "body": {"mimeType": "text/html", "textContent": body_text},
                    "createdAt": created_at_epoch,
                    "updatedAt": updated_at_epoch,
                    "tags": tags + [datasource],
                    "customProperties": custom_properties,
                    "permissions": permissions,
                }
                documents.append(document)

            # Build the payload for this batch.
            batch_body: Dict[str, Any] = {
                "uploadId": upload_id,
                "datasource": datasource,
                "documents": documents,
                "disableStaleDocumentDeletionCheck": deleteStale
            }

            # If using the bulk endpoint, add additional paging fields.
            if bulk:
                batch_body["isFirstPage"] = (batch_num == 1)
                batch_body["isLastPage"] = (batch_num == total_batches)
                # Typically, forceRestartUpload is set on the first batch.
                if batch_num == 1:
                    batch_body["forceRestartUpload"] = True

            # Send the request with retry logic.
            retries = 0
            while retries <= max_retries:
                logger.info(
                    "Indexing batch %d (uploadId=%s, attempt %d/%d)...",
                    batch_num,
                    upload_id,
                    retries + 1,
                    max_retries,
                )
                resp = requests.post(endpoint, headers=self.headers, json=batch_body, verify=self.verify_ssl)
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        wait_time = int(retry_after)
                        logger.warning("Rate limit exceeded. Retrying after %s seconds...", wait_time)
                        time.sleep(wait_time)
                        retries += 1
                        continue
                    else:
                        raise Exception(f"Rate limit exceeded but no 'Retry-After' header for batch {batch_num}.")
                _ = self._handle_response(resp)
                logger.info("Batch %d indexed successfully. Sleeping for %.1f seconds...", batch_num, post_batch_sleep)
                time.sleep(post_batch_sleep)
                break
            else:
                raise Exception(f"Failed to index batch {batch_num} after {max_retries} retries.")

    # -------------------------------------------------------------------
    # 6) Get Document Count
    # -------------------------------------------------------------------
    def get_document_count(self, datasource: str) -> int:
        """
        Retrieve the document count for a given datasource.

        Args:
            datasource (str): Name of the datasource.

        Returns:
            int: The document count.
        """
        endpoint = f"{self.host}/getdocumentcount"
        body = {"datasource": datasource}

        logger.info("Fetching document count for datasource '%s'...", datasource)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        data = self._handle_response(resp)
        return data.get("documentCount", 0)

    # -------------------------------------------------------------------
    # 7) Delete a Document
    # -------------------------------------------------------------------
    def delete_document(
        self,
        doc_id: str,
        datasource: str,
        object_type: str,
        is_async: bool = False,
    ) -> None:
        """
        Delete a document from a datasource.

        Args:
            doc_id (str): Unique document ID.
            datasource (str): Datasource name.
            object_type (str): Object type of the document.
            is_async (bool, optional): Flag indicating asynchronous deletion (for logging only).
        """
        endpoint = f"{self.host}/deletedocument"
        body = {
            "datasource": datasource,
            "objectType": object_type,
            "id": doc_id,
        }

        logger.info("Deleting document '%s' (async=%s)...", doc_id, is_async)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        data = self._handle_response(resp)
        logger.info("Delete call response: %s", data)

    # -------------------------------------------------------------------
    # 8) Get Document Status
    # -------------------------------------------------------------------
    def get_document_status(
        self, datasource: str, object_type: str, doc_id: str
    ) -> Dict[str, Any]:
        """
        Retrieve the status of a specific document.

        Args:
            datasource (str): Datasource name.
            object_type (str): Object type of the document.
            doc_id (str): Unique document ID.

        Returns:
            Dict[str, Any]: Document status information.
        """
        endpoint = f"{self.host}/getdocumentstatus"
        body = {
            "datasource": datasource,
            "objectType": object_type,
            "docId": doc_id,
        }

        logger.info("Fetching document status for doc_id='%s' in datasource '%s'...", doc_id, datasource)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        return self._handle_response(resp)

    # -------------------------------------------------------------------
    # 9) Process Documents
    # -------------------------------------------------------------------
    def process_documents(self, datasource: Optional[str] = None) -> Any:
        """
        Trigger processing of uploaded documents.

        Calls the /processalldocuments endpoint to schedule document processing.
        If a datasource is specified, only that datasource is processed; otherwise,
        documents for all datasources are processed.

        Note: This endpoint is rate-limited (typically once per datasource every 3 hours).

        Args:
            datasource (Optional[str]): Datasource name (optional).

        Returns:
            Any: Response from the API.
        """
        endpoint = f"{self.host}/processalldocuments"
        body: Dict[str, Any] = {}
        if datasource:
            body["datasource"] = datasource

        logger.info("Processing documents%s...",
                    f" for datasource '{datasource}'" if datasource else " for all datasources")
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        return self._handle_response(resp)

    # -------------------------------------------------------------------
    # 10) Clear Datasource
    # -------------------------------------------------------------------
    def clear_datasource(self, datasource: str) -> None:
        """
        Clear all documents from a datasource.

        This method performs a bulk indexing operation with an empty list (which deletes
        all documents in the datasource), then triggers processing for that datasource.

        Args:
            datasource (str): The datasource to clear.
        """
        logger.info("Clearing datasource '%s'...", datasource)
        self.index_items(
            items=[],
            datasource=datasource,
            upload_id_prefix="clear_ds",
            bulk=True
        )
        logger.info("Triggering processing for datasource '%s'...", datasource)
        response = self.process_documents(datasource=datasource)
        logger.info("Process documents response: %s", response)
