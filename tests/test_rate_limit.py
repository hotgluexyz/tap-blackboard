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


def _grades_stream(**config_overrides) -> GradesStream:
    """Return a grades stream with optional config overrides."""
    tap = Tapblackboard(config={**SAMPLE_CONFIG, **config_overrides})
    return GradesStream(tap)


@pytest.fixture
def grades_stream() -> GradesStream:
    """Return a grades stream with skip_429s enabled."""
    return _grades_stream(skip_429s=True)


def test_skip_429s_reads_top_level_config():
    """Hotglue connect_ui_params values are flattened onto tap config."""
    stream = _grades_stream(skip_429s=True)
    assert stream._skip_429s_enabled() is True


def test_skip_429s_defaults_off():
    """Without the flag, 429s are not soft-stopped."""
    stream = _grades_stream()
    assert stream._skip_429s_enabled() is False


def test_wait_generator_honors_capped_retry_after(grades_stream: GradesStream):
    """Retry-After above the cap should not park the job for hours."""
    grades_stream._last_retry_after = 7200
    wait = grades_stream.backoff_wait_generator()
    assert next(wait) == MAX_RETRY_AFTER_SECONDS


def test_wait_generator_honors_short_retry_after(grades_stream: GradesStream):
    """Short Retry-After values should be used as-is."""
    grades_stream._last_retry_after = 12
    wait = grades_stream.backoff_wait_generator()
    assert next(wait) == 12


def test_wait_generator_falls_back_to_expo(grades_stream: GradesStream):
    """Without Retry-After, the first wait comes from exponential backoff."""
    grades_stream._last_retry_after = None
    wait = grades_stream.backoff_wait_generator()
    assert next(wait) == 2  # backoff.expo(factor=2) starts at 2 * 2**0


def test_wait_generator_never_yields_none(grades_stream: GradesStream):
    """backoff 1.x passes each yield into full_jitter(value); None breaks it."""
    wait = grades_stream.backoff_wait_generator()
    for _ in range(5):
        value = next(wait)
        assert value is not None
        assert isinstance(value, int)
        assert value > 0


def test_child_stream_skips_403_without_raising(grades_stream: GradesStream):
    """403 partitions remain soft-skipped."""
    response = requests.Response()
    response.status_code = 403
    response.url = "https://example.test/forbidden"

    grades_stream.validate_response(response)
    assert list(grades_stream.parse_response(response)) == []
    assert grades_stream.get_next_page_token(response, None) is None


def test_skip_429s_soft_stops_on_first_429(grades_stream: GradesStream):
    """With skip_429s, the first 429 trips the circuit without raising."""
    response = requests.Response()
    response.status_code = 429
    response.url = (
        "https://example.test/learn/api/public/v2/courses/_1/gradebook/"
        "columns/_2/users"
    )
    response.headers["X-Rate-Limit-Remaining"] = "0"
    response.headers["Retry-After"] = "3600"

    grades_stream.validate_response(response)

    assert blackboardStream._rate_limit_tripped is True
    assert list(grades_stream.parse_response(response)) == []
    assert grades_stream.get_next_page_token(response, None) is None


def test_without_skip_429s_raises_on_429():
    """When skip_429s is off, 429 remains a retriable/fatal SDK error path."""
    stream = _grades_stream(skip_429s=False)
    response = requests.Response()
    response.status_code = 429
    response.reason = "Too Many Requests"
    response.url = "https://example.test/rate-limited"

    with pytest.raises(RetriableAPIError):
        stream.validate_response(response)

    assert blackboardStream._rate_limit_tripped is False


def test_request_records_skips_without_http_when_circuit_tripped(
    grades_stream: GradesStream,
    monkeypatch: pytest.MonkeyPatch,
):
    """Once tripped under skip_429s, later partitions should not call the API."""
    blackboardStream._rate_limit_tripped = True
    called = {"super": False}

    def should_not_run(self, context):  # noqa: ARG001
        called["super"] = True
        if False:  # pragma: no cover
            yield {}

    monkeypatch.setattr(RESTStream, "request_records", should_not_run)

    assert list(grades_stream.request_records({"course_id": "_1"})) == []
    assert called["super"] is False
