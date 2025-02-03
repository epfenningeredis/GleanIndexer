import requests
import time
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Union, Optional
from dateutil import parser as date_parser

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GleanIndexer:
    """
    A wrapper class for interacting with the Glean Indexing API using requests.

    Attributes:
        api_key (str): The API key (Indexing Token) used for authentication.
        host (str): The API host URL, typically https://{your-domain}-be.glean.com/api/index/v1.
        verify_ssl (bool): Whether to verify SSL certificates.
        headers (dict): Default headers used for requests.
    """

    def __init__(self, api_key: str, host: str, verify_ssl: bool = True):
        """
        Initialize the GleanIndexer with API credentials, host, and SSL verification preference.
        """
        self.api_key = api_key
        self.host = host.rstrip("/")  # Ensure no trailing slash
        self.verify_ssl = verify_ssl

        # Common headers for all requests
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _handle_response(self, response: requests.Response) -> Any:
        """
        Handle the HTTP response from a requests call.
        - Returns the response (or JSON) for a 429 so that the caller can retry.
        - Raises for other non-200 statuses.
        """
        if response.status_code == 429:
            return response  # Caller should handle rate limit retries

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
        - If value is None, returns current time.
        - If value is an int, returns it.
        - If value is a string, attempts to parse it.
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

    # -------------------------------------------------------
    # 1) Create or Update a Data Source
    # -------------------------------------------------------
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

        logger.info("Creating/Updating datasource '%s'...", display_name)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        data = self._handle_response(resp)

        if resp.status_code == 200:
            logger.info("Datasource '%s' created/updated successfully.", display_name)
        else:
            raise Exception(f"Failed to create/update datasource '{display_name}'. Response: {data}")

    # -------------------------------------------------------
    # 2) Retrieve the datasource config
    # -------------------------------------------------------
    def get_datasource_config(self, datasource_name: str) -> Any:
        endpoint = f"{self.host}/getdatasourceconfig"
        body = {"datasource": datasource_name}

        logger.info("Retrieving config for datasource '%s'...", datasource_name)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        return self._handle_response(resp)

    # -------------------------------------------------------
    # 3) Add allowed (greenlist) users to a datasource
    # -------------------------------------------------------
    def add_allowed_users(self, datasource_name: str, emails: List[str]) -> None:
        if not emails:
            raise ValueError("At least one email address must be provided.")

        endpoint = f"{self.host}/betausers"
        body = {"datasource": datasource_name, "emails": emails}

        logger.info("Adding allowed (beta) users to datasource '%s': %s", datasource_name, emails)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        data = self._handle_response(resp)

        if resp.status_code == 200:
            logger.info("Successfully allowed users %s for datasource '%s'.", emails, datasource_name)
        else:
            raise Exception(f"Failed to add allowed users. Response: {data}")

    # -------------------------------------------------------
    # 4) Index a Single Document
    # -------------------------------------------------------
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
        custom_fields: Optional[List[Dict[str, Any]]] = None,
        max_retries: int = 5,
    ) -> Any:
        endpoint = f"{self.host}/indexdocument"
        tags = tags or []

        if not permissions:
            permissions = {"allowAnonymousAccess": True}

        created_at_epoch = self.to_epoch(created_at)
        updated_at_epoch = self.to_epoch(updated_at)

        custom_properties = custom_fields or []

        body = {
            "document": {
                "datasource": datasource,
                "objectType": object_type,
                "title": name,
                "id": doc_id,
                "createdAt": created_at_epoch,
                "updatedAt": updated_at_epoch,
                "tags": tags + [datasource],
                "viewURL": url,
                "summary": {"mimeType": "text/html", "textContent": description},
                "customProperties": custom_properties,
                "permissions": permissions,
            }
        }

        retries = 0
        while retries <= max_retries:
            logger.info("Indexing document '%s' (attempt %d/%d)...", doc_id, retries + 1, max_retries)
            resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
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

    # -------------------------------------------------------
    # 5) Index Multiple Documents (Bulk Indexing)
    # -------------------------------------------------------
        # -------------------------------------------------------
    # 5) Index Multiple Documents (Bulk/Incremental Indexing)
    # -------------------------------------------------------
    def index_items(
        self,
        items: List[Dict[str, Any]],
        datasource: str,
        upload_id_prefix: str,
        bulk: bool = False,
        max_retries: int = 5,
        batch_size: int = 600,
        post_batch_sleep: float = 15.0,
    ) -> None:
        """
        Index multiple documents in batches.

        Args:
            items (List[Dict[str, Any]]): A list of items where each item is a dict containing:
                - id (str): required
                - name (str): required
                - object_type (str): required
                - url (str): required
                - summary (str): optional
                - body (str): optional
                - tags (List[str]): optional
                - permissions (dict): optional
                - created_at (Union[int, str]): optional; can be an epoch or date string
                - custom_fields (List[Dict[str, Any]]): optional, list of dicts with keys 'name' and 'value'
            datasource (str): Data source name.
            upload_id_prefix (str): Prefix used in generating the unique uploadId per batch.
            bulk (bool): If False (default), uses the `/indexdocuments` endpoint which indexes documents incrementally 
                (i.e. does not delete documents that are not present). If True, uses the `/bulkindexdocuments` endpoint,
                which replaces the documents in the datasource (i.e. deletes documents not present in the current batch).
            max_retries (int): Retry count for rate-limiting.
            batch_size (int): Number of documents per batch.
            post_batch_sleep (float): Sleep time after each batch to mitigate rate limits.
        """
        # Choose the endpoint based on the bulk flag
        endpoint = f"{self.host}/bulkindexdocuments" if bulk else f"{self.host}/indexdocuments"

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
                    raise ValueError("Each item must have 'id', 'name', 'object_type', and 'url'.")

                summary = item.get("summary", "")
                body_text = item.get("body", "")
                tags = item.get("tags", [])
                permissions = item.get("permissions") or {"allowAnonymousAccess": True}
                created_at_value = item.get("created_at")
                created_at_epoch = self.to_epoch(created_at_value)
                # For bulk indexing, we use the current time as updatedAt.
                updated_at_epoch = current_time_epoch

                # Expect custom_fields as a list; if not provided, default to an empty list.
                custom_properties = item.get("custom_fields") or []

                documents.append(
                    {
                        "datasource": datasource,
                        "objectType": object_type,
                        "title": name,
                        "id": doc_id,
                        "createdAt": created_at_epoch,
                        "updatedAt": updated_at_epoch,
                        "tags": tags + [datasource],
                        "viewURL": url,
                        "summary": {"mimeType": "text/html", "textContent": summary},
                        "body": {"mimeType": "text/html", "textContent": body_text},
                        "customProperties": custom_properties,
                        "permissions": permissions,
                    }
                )

            batch_body = {
                "uploadId": upload_id,
                "datasource": datasource,
                "documents": documents,
            }

            retries = 0
            while retries <= max_retries:
                logger.info("Indexing batch %d with uploadId=%s (attempt %d/%d)...",
                            batch_num, upload_id, retries + 1, max_retries)
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
                        raise Exception(f"Rate limit exceeded but 'Retry-After' header missing for batch {batch_num}.")
                data = self._handle_response(resp)
                logger.info("Batch %d indexed successfully. Sleeping %.1f seconds...", batch_num, post_batch_sleep)
                time.sleep(post_batch_sleep)
                break
            else:
                raise Exception(
                    f"Failed to index documents in batch {batch_num} after {max_retries} retries due to rate limiting."
                )


    # -------------------------------------------------------
    # 6) Get Document Count
    # -------------------------------------------------------
    def get_document_count(self, datasource_name: str) -> int:
        endpoint = f"{self.host}/getdocumentcount"
        body = {"datasource": datasource_name}

        logger.info("Fetching document count for datasource '%s'...", datasource_name)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        data = self._handle_response(resp)
        return data.get("documentCount", 0)

    # -------------------------------------------------------
    # 7) Delete a Document
    # -------------------------------------------------------
    def delete_document(
        self,
        doc_id: str,
        datasource: str,
        object_type: str,
        is_async: bool = False,
    ) -> None:
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

    # -------------------------------------------------------
    # 8) Get Document Status
    # -------------------------------------------------------
    def get_document_status(
        self, datasource_name: str, object_type: str, doc_id: str
    ) -> Dict[str, Any]:
        endpoint = f"{self.host}/getdocumentstatus"
        body = {
            "datasource": datasource_name,
            "objectType": object_type,
            "docId": doc_id,
        }

        logger.info("Fetching document status for doc_id='%s' in datasource '%s'...", doc_id, datasource_name)
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        return self._handle_response(resp)
    
    def process_documents(self, datasource: Optional[str] = None) -> Any:
        """
        Schedule the immediate processing of uploaded documents.

        This method calls the `/processalldocuments` endpoint, which schedules the processing
        of documents that have been uploaded through the indexing API. If a datasource is provided,
        only the documents for that datasource are processed. Otherwise, documents for all datasources
        are processed.

        Note: This endpoint is rate-limited (typically one call per datasource every 3 hours).

        Args:
            datasource (Optional[str]): The datasource to process documents for.
                                       If omitted, processes documents for all datasources.

        Returns:
            Any: The response from the API.
        """
        endpoint = f"{self.host}/processalldocuments"
        body = {}
        if datasource is not None:
            body["datasource"] = datasource

        logger.info("Processing documents%s...",
                    f" for datasource '{datasource}'" if datasource else " for all datasources")
        resp = requests.post(endpoint, headers=self.headers, json=body, verify=self.verify_ssl)
        return self._handle_response(resp)
    
    # -------------------------------------------------------
    # 10) Clear Datasource
    # -------------------------------------------------------
    def clear_datasource(self, datasource: str) -> None:
        """
        Clear the contents of a datasource.

        This method clears a datasource by performing a bulk indexing operation
        with an empty list of documents (thereby deleting documents not present in the payload).
        Immediately after, it triggers the processing of documents for that datasource.

        Args:
            datasource (str): The datasource to clear.
        """
        logger.info("Clearing datasource '%s'...", datasource)
        # Call bulk index with an empty list. An upload_id_prefix is provided for tracking.
        self.index_items(
            items=[],
            datasource=datasource,
            upload_id_prefix="clear_ds",
            bulk=True  # Use the bulk indexing endpoint
        )
        logger.info("Triggering processing for datasource '%s'...", datasource)
        response = self.process_documents(datasource=datasource)
        logger.info("Process documents response: %s", response)
    
    
