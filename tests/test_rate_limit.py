"""Tests for Blackboard client rate-limit handling."""

from __future__ import annotations

import datetime

import pytest
import requests
from hotglue_singer_sdk.exceptions import RetriableAPIError
from hotglue_singer_sdk.streams import RESTStream

from tap_blackboard.client import (
    MAX_RETRY_AFTER_SECONDS,
    blackboardStream,
)
from tap_blackboard.streams import GradesStream
from tap_blackboard.tap import Tapblackboard

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "api_url": "https://bd-partner-a-original.blackboard.com",
    "client_id": "placeholder",
    "client_secret": "placeholder",
}


@pytest.fixture(autouse=True)
def _reset_rate_limit_flag():
    """Ensure the process-wide circuit breaker starts open for each test."""
    blackboardStream._rate_limit_tripped = False
    yield
    blackboardStream._rate_limit_tripped = False


@pytest.fixture
def grades_stream() -> GradesStream:
    """Return a grades stream instance for unit tests."""
    tap = Tapblackboard(config=SAMPLE_CONFIG)
    return GradesStream(tap)


def test_rate_limit_wait_honors_retry_after_cap():
    """Retry-After above the cap should not park the job for hours."""
    response = requests.Response()
    response.status_code = 429
    response.headers["Retry-After"] = "7200"
    exc = RetriableAPIError("429", response)

    assert (
        blackboardStream._rate_limit_wait_seconds(exc) == MAX_RETRY_AFTER_SECONDS
    )


def test_rate_limit_wait_honors_short_retry_after():
    """Short Retry-After values should be used as-is."""
    response = requests.Response()
    response.status_code = 429
    response.headers["Retry-After"] = "12"
    exc = RetriableAPIError("429", response)

    assert blackboardStream._rate_limit_wait_seconds(exc) == 12


def test_rate_limit_wait_defaults_without_header():
    """Missing Retry-After falls back to a short fixed delay."""
    response = requests.Response()
    response.status_code = 429
    exc = RetriableAPIError("429", response)

    assert blackboardStream._rate_limit_wait_seconds(exc) == 5


def test_child_stream_skips_403_without_raising(grades_stream: GradesStream):
    """403 partitions remain soft-skipped."""
    response = requests.Response()
    response.status_code = 403
    response.url = "https://example.test/forbidden"

    grades_stream.validate_response(response)
    assert list(grades_stream.parse_response(response)) == []
    assert grades_stream.get_next_page_token(response, None) is None


def test_request_records_trips_circuit_on_exhausted_429(
    grades_stream: GradesStream,
    monkeypatch: pytest.MonkeyPatch,
):
    """After retries are exhausted, 429 should trip the circuit and skip."""
    response = requests.Response()
    response.status_code = 429
    response.url = (
        "https://example.test/learn/api/public/v2/courses/_1/gradebook/"
        "columns/_2/users"
    )
    response.headers["X-Rate-Limit-Remaining"] = "0"
    response.headers["X-Rate-Limit-Limit"] = "10000"
    response.headers["Retry-After"] = "3600"

    def boom(self, context):  # noqa: ARG001
        raise RetriableAPIError("429 Client Error", response)
        yield  # pragma: no cover

    monkeypatch.setattr(RESTStream, "request_records", boom)

    assert list(grades_stream.request_records({"course_id": "_1"})) == []
    assert blackboardStream._rate_limit_tripped is True


def test_request_records_skips_without_http_when_circuit_tripped(
    grades_stream: GradesStream,
    monkeypatch: pytest.MonkeyPatch,
):
    """Once tripped, later partitions should not call the API again."""
    blackboardStream._rate_limit_tripped = True
    called = {"super": False}

    def should_not_run(self, context):  # noqa: ARG001
        called["super"] = True
        if False:  # pragma: no cover
            yield {}

    monkeypatch.setattr(RESTStream, "request_records", should_not_run)

    assert list(grades_stream.request_records({"course_id": "_1"})) == []
    assert called["super"] is False


def test_tripped_circuit_allows_429_through_validate(grades_stream: GradesStream):
    """After the circuit trips, validate_response must not re-raise 429."""
    blackboardStream._rate_limit_tripped = True
    response = requests.Response()
    response.status_code = 429
    response.url = "https://example.test/rate-limited"

    grades_stream.validate_response(response)
    assert list(grades_stream.parse_response(response)) == []
