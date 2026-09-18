"""blackboard tap class."""

from __future__ import annotations

from typing import Any

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th  # JSON schema typing helpers
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from typing_extensions import override

from tap_blackboard.auth import blackboardAuthenticator
from tap_blackboard.client import DEFAULT_API_URL
from tap_blackboard.streams import (
    AnnouncementsStream,
    CourseAnnouncementsStream,
    CoursesStream,
    EnrollmentsStream,
    GradebookColumnsStream,
    GradesStream,
)

STREAM_TYPES = [
    CoursesStream,
    EnrollmentsStream,
    GradebookColumnsStream,
    GradesStream,
    AnnouncementsStream,
    CourseAnnouncementsStream,
]


class Tapblackboard(Tap):
    """Singer tap for blackboard."""

    name = "tap-blackboard"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
            default="2000-01-01T00:00:00Z",
        ),
        th.Property(
            "api_url",
            th.StringType,
            description="Blackboard Learn host root URL (e.g. https://learn.example.com)",
            default=DEFAULT_API_URL,
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="OAuth application key from the Blackboard developer portal",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="OAuth application secret from the Blackboard developer portal",
        ),
        th.Property(
            "course_ids",
            th.ArrayType(th.StringType),
            description=(
                "Optional list of Learn course primary keys (e.g. _7_1) to limit "
                "sync to specific courses and their child streams"
            ),
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @classmethod
    def access_token_support(
        cls,
        connector: Any = None,
    ) -> tuple[type[OAuthAuthenticator], str]:
        """Return the authenticator class and OAuth token endpoint.

        Returns:
            A tuple with the authenticator class and the OAuth token endpoint URL.
        """
        api_url = DEFAULT_API_URL
        if connector is not None:
            api_url = connector.config.get("api_url", DEFAULT_API_URL)
        token_url = f"{api_url.rstrip('/')}/learn/api/public/v1/oauth2/token"
        return blackboardAuthenticator, token_url


if __name__ == "__main__":
    Tapblackboard.cli()
