"""Stream type classes for tap-dataverse."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable
from urllib.parse import parse_qsl

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams.core import REPLICATION_INCREMENTAL

from tap_dataverse.auth import DataverseAuthenticator
from tap_dataverse.client import DataverseStream
from tap_dataverse.utils import sql_attribute_name

if TYPE_CHECKING:
    from urllib.parse import ParseResult

    import requests

    from tap_dataverse.tap import TapDataverse


class DataverseTableStream(DataverseStream):
    """Customised stream for any Dataverse Table."""

    def __init__(
        self,
        tap: TapDataverse,
        stream_config: dict,
        entity_set_name: str,
        schema: dict | None = None,
    ) -> None:
        """Init DataverseTableStream."""
        name = stream_config.get("name")
        path = f"/{entity_set_name}"
        super().__init__(tap=tap, name=name, schema=schema, path=path)

        self.tap = tap
        self.name = name
        self.path = path
        self.records_path = "$.value[*]"

       # Store custom query params from stream config
        self.custom_query_params = stream_config.get("query_params", "")

        self.start_date = stream_config.get(
            "start_date", tap.config.get("start_date", "")
        )
        # Use the property setter to ensure proper replication method detection
        replication_key_value = stream_config.get(
            "replication_key", tap.config.get("replication_key", "")
        )
        if replication_key_value:
            self.replication_key = replication_key_value
    
    def get_starting_timestamp(self, context: dict | None) -> datetime.datetime | None:
        """Get starting replication timestamp with proper start_date fallback.
        
        This method fixes the issue where start_date is not properly used
        when no state exists.
        """
        from datetime import datetime
        import pendulum
        
        # First try to get from state
        value = self.get_starting_replication_key_value(context)
        
        # If no state value, use start_date if available
        if value is None and self.start_date:
            try:
                return pendulum.parse(self.start_date)
            except Exception:
                self.logger.warning(f"Invalid start_date format: {self.start_date}")
                return None
        
        # If we have a value, validate it's a timestamp
        if value is not None:
            if not self.is_timestamp_replication_key:
                msg = f"The replication key {self.replication_key} is not of timestamp type"
                raise ValueError(msg)
            return pendulum.parse(value)
        
        return None
        
    @property
    def is_sorted(self) -> bool:
        """Expect stream to be sorted.

        When `True`, incremental streams will attempt to resume if unexpectedly
        interrupted.

        Returns:
            `True` if stream is sorted. Defaults to `False`.
        """
        # Set is sorted based on INCREMENTAL stream replication type
        return self.replication_method == REPLICATION_INCREMENTAL

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = super().http_headers
        if self.config.get("annotations"):
            headers["Prefer"] = 'odata.include-annotations="*"'
        return headers

    @property
    def authenticator(self) -> DataverseAuthenticator:
        """Return a DataverseAuthenticator object for this stream."""
        return DataverseAuthenticator(stream=self)

    def get_url_params(
        self, context: dict | None, next_page_token: ParseResult | None
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: optional - the singer context object.
            next_page_token: optional - the token for the next page of results.

        Returns:
            An object containing the parameters to add to the request.

        """
        # Initialise Starting Values
        try:
            timestamp = self.get_starting_timestamp(context)
            if timestamp:
                last_run_date = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                # Fallback to start_date if no timestamp from state
                last_run_date = self.start_date if self.start_date else None
        except (ValueError, AttributeError):
            last_run_date = self.get_starting_replication_key_value(context)
            # If still None, try start_date
            if last_run_date is None and self.start_date:
                last_run_date = self.start_date

        params: dict = {}

        if self.replication_key:
            params["$orderby"] = f"{self.replication_key} asc"
            if last_run_date:
                params["$filter"] = f"{self.replication_key} ge {last_run_date}"

        # Add custom query params from stream config
        if self.custom_query_params:
            # Parse the custom query params string (e.g., "$filter=objectidtypecode eq 'contact'")
            for param in self.custom_query_params.split("&"):
                if "=" in param:
                    key, value = param.split("=", 1)
                    # If $filter already exists, combine them with 'and'
                    if key == "$filter" and "$filter" in params:
                        params["$filter"] = f"({params['$filter']}) and ({value})"
                    else:
                        params[key] = value

        if next_page_token:
            # Only provide the skiptoken on subsequent requests
            self.logger.info(next_page_token.query)
            params = dict(parse_qsl(next_page_token.query))

        self.logger.info(params)
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: required - the requests.Response given by the api call.

        Yields:
              Parsed records.

        """
        yield from extract_jsonpath(self.records_path, input=response.json())

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        if self.config.get("sql_attribute_names"):
            """
            SQL identifiers and key words must begin with a letter (a-z, but
            also letters with diacritical marks and non-Latin letters) or an
            underscore (_). Subsequent characters in an identifier or key word
            can be letters, underscores, digits (0-9), or dollar signs ($). Note
            that dollar signs are not allowed in identifiers according to the
            letter of the SQL standard, so their use might render applications
            less portable
            """
            row = {sql_attribute_name(k): v for k, v in row.items()}

        return row