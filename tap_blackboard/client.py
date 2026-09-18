"""HTTP API client, including blackboardStream base class."""

from __future__ import annotations

from collections.abc import Iterable
from functools import cached_property
from typing import Any
from urllib.parse import urljoin

import requests
from hotglue_singer_sdk.authenticators import APIAuthenticatorBase
from hotglue_singer_sdk.streams import RESTStream
from typing_extensions import override

DEFAULT_API_URL = "https://bd-partner-a-original.blackboard.com"
PAGE_SIZE = 100


class blackboardStream(RESTStream):
    """blackboard stream class."""

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.nextPage"
    page_size = PAGE_SIZE

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via the ``api_url`` tap setting."""
        host = self.config.get("api_url", DEFAULT_API_URL).rstrip("/")
        return f"{host}/learn/api/public"

    @override
    @cached_property
    def authenticator(self) -> APIAuthenticatorBase:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        authenticator_cls, auth_endpoint = self._tap.access_token_support(self._tap)
        return authenticator_cls(self, auth_endpoint=auth_endpoint)

    @override
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        return {"Accept": "application/json"}

    @override
    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return the ``paging.nextPage`` path/URL, or None when done.

        Args:
            response: A raw `requests.Response`_ object.
            previous_token: Previous pagination reference.

        Returns:
            Next-page URL path from the response body, if present.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        return response.json().get("paging", {}).get("nextPage")

    @override
    def prepare_request(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> requests.PreparedRequest:
        """Build the request, following Blackboard ``paging.nextPage`` URLs as-is.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Full path/URL from ``paging.nextPage``, or None.

        Returns:
            A prepared HTTP request.
        """
        if next_page_token:
            host = self.config.get("api_url", DEFAULT_API_URL).rstrip("/") + "/"
            if str(next_page_token).startswith("http"):
                url = str(next_page_token)
            else:
                url = urljoin(host, str(next_page_token).lstrip("/"))
            return self.build_prepared_request(
                method=self.rest_method,
                url=url,
                params={},
                headers=self.http_headers,
                json=None,
            )
        return super().prepare_request(context, next_page_token)

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return query params for the first page of a list endpoint.

        Args:
            context: The stream context.
            next_page_token: Unused on first page (next pages use prepare_request).

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, Any] = {"limit": self.page_size}
        if self.replication_key:
            start_date = self.get_starting_time(context)
            if start_date:
                params["modified"] = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                params["modifiedCompare"] = "greaterOrEqual"
        return params


class blackboardChildStream(blackboardStream):
    """Course/column-scoped stream that skips partitions denied with HTTP 403."""

    @override
    def validate_response(self, response: requests.Response) -> None:
        """Allow 403 through so the partition can be skipped instead of failing."""
        if response.status_code == 403:
            return
        super().validate_response(response)

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield no records when Blackboard forbids this partition."""
        if response.status_code == 403:
            url = response.url or (response.request.url if response.request else "")
            self.logger.warning(
                "Skipping stream %r: HTTP 403 Forbidden for %s",
                self.name,
                url,
            )
            return
        yield from super().parse_response(response)

    @override
    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Stop paging after a forbidden response."""
        if response.status_code == 403:
            return None
        return super().get_next_page_token(response, previous_token)
