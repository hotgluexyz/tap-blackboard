"""HTTP API client, including blackboardStream base class."""

from __future__ import annotations

from collections.abc import Generator, Iterable
from functools import cached_property
from typing import Any, ClassVar
from urllib.parse import urljoin

import backoff
import requests
from hotglue_singer_sdk.authenticators import APIAuthenticatorBase
from hotglue_singer_sdk.exceptions import RetriableAPIError
from hotglue_singer_sdk.streams import RESTStream
from typing_extensions import override

DEFAULT_API_URL = "https://bd-partner-a-original.blackboard.com"
PAGE_SIZE = 100
# Daily Blackboard quotas can set Retry-After to many hours; do not park the job.
MAX_RETRY_AFTER_SECONDS = 60
RATE_LIMIT_MAX_TRIES = 7


class blackboardStream(RESTStream):
    """blackboard stream class."""

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.nextPage"
    page_size = PAGE_SIZE
    # Shared across all stream instances for the process lifetime of a sync.
    _rate_limit_tripped: ClassVar[bool] = False
    _last_retry_after: int | None = None

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

    def _skip_429s_enabled(self) -> bool:
        """Return whether config asks to soft-stop on 429.

        Hotglue ``connect_ui_params.skip_429s`` is written as top-level
        ``skip_429s`` on the connector config passed to the tap.
        """
        return bool(self.config.get("skip_429s"))

    @override
    def backoff_max_tries(self) -> int:
        """Retry rate limits unless skip_429s is on (then fail fast into soft-stop)."""
        if self._skip_429s_enabled():
            return 1
        return RATE_LIMIT_MAX_TRIES

    @override
    def backoff_wait_generator(self) -> Generator[int, None, None]:
        """Yield wait times for backoff 1.x (``next()`` only, not ``.send``).

        Prefer Blackboard ``Retry-After`` when present, otherwise exponential
        backoff. Waits are capped so daily-quota resets do not hang the job.
        """
        return self._retry_after_or_expo()

    def _retry_after_or_expo(self) -> Generator[int, None, None]:
        """Wait generator compatible with ``backoff==1.x`` / ``full_jitter(value)``."""
        expo = backoff.expo(factor=2, max_value=MAX_RETRY_AFTER_SECONDS)
        while True:
            retry_after = self._last_retry_after
            if retry_after is not None:
                self._last_retry_after = None
                yield min(max(retry_after, 1), MAX_RETRY_AFTER_SECONDS)
            else:
                yield int(next(expo))

    def _capture_retry_after(self, response: requests.Response) -> None:
        """Stash Retry-After from a 429 so the wait generator can honor it."""
        if response.status_code != 429:
            return
        raw = response.headers.get("Retry-After")
        if raw is None:
            self._last_retry_after = None
            return
        try:
            self._last_retry_after = int(float(str(raw).strip()))
        except ValueError:
            self._last_retry_after = None

    @override
    def validate_response(self, response: requests.Response) -> None:
        """On skip_429s, soft-accept the first 429 and stop further API work."""
        self._capture_retry_after(response)
        if response.status_code == 429 and self._skip_429s_enabled():
            if not blackboardStream._rate_limit_tripped:
                self._trip_rate_limit(response, None)
            return
        if response.status_code == 429 and blackboardStream._rate_limit_tripped:
            return
        super().validate_response(response)

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield no records when a 429 soft-stop is in effect."""
        if response.status_code == 429 and blackboardStream._rate_limit_tripped:
            url = response.url or (response.request.url if response.request else "")
            self.logger.warning(
                "Skipping stream %r: HTTP 429 for %s",
                self.name,
                url,
            )
            return
        yield from super().parse_response(response)

    @override
    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Request records; when skip_429s tripped, end the sync without more calls."""
        if blackboardStream._rate_limit_tripped and self._skip_429s_enabled():
            self.logger.warning(
                "Skipping stream %r partition %s: skip_429s stopped sync after "
                "rate limit",
                self.name,
                context,
            )
            return

        try:
            yield from super().request_records(context)
        except RetriableAPIError as exc:
            response = getattr(exc, "response", None)
            if (
                response is not None
                and response.status_code == 429
                and self._skip_429s_enabled()
            ):
                self._trip_rate_limit(response, context)
                return
            raise

    def _trip_rate_limit(
        self,
        response: requests.Response,
        context: dict | None,
    ) -> None:
        """Mark the sync as rate-limited and log quota headers."""
        blackboardStream._rate_limit_tripped = True
        url = response.url or (response.request.url if response.request else "")
        remaining = response.headers.get("X-Rate-Limit-Remaining")
        limit = response.headers.get("X-Rate-Limit-Limit")
        retry_after = response.headers.get("Retry-After")
        self.logger.warning(
            "skip_429s: stopping sync after HTTP 429 for stream %r partition %s "
            "(%s; X-Rate-Limit-Remaining=%s, X-Rate-Limit-Limit=%s, "
            "Retry-After=%s). Records already emitted will proceed to ETL.",
            self.name,
            context,
            url,
            remaining,
            limit,
            retry_after,
        )

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
        if response.status_code == 429 and blackboardStream._rate_limit_tripped:
            return None
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
    """Course/column-scoped stream that skips partitions denied with HTTP 403/429."""

    @override
    def validate_response(self, response: requests.Response) -> None:
        """Allow 403 through; soft-accept 429 when skip_429s has tripped the sync."""
        if response.status_code == 403:
            return
        if response.status_code == 429 and blackboardStream._rate_limit_tripped:
            self._capture_retry_after(response)
            return
        super().validate_response(response)

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Yield no records when Blackboard forbids or rate-limits this partition."""
        if response.status_code in (403, 429):
            url = response.url or (response.request.url if response.request else "")
            self.logger.warning(
                "Skipping stream %r: HTTP %s for %s",
                self.name,
                response.status_code,
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
        """Stop paging after a forbidden or rate-limited response."""
        if response.status_code in (403, 429):
            return None
        return super().get_next_page_token(response, previous_token)
