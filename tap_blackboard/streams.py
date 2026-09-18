"""Stream type classes for tap-blackboard."""

from __future__ import annotations

from typing import Any, ClassVar

from hotglue_singer_sdk import typing as th  # JSON Schema typing helpers
from typing_extensions import override

from tap_blackboard.client import blackboardChildStream, blackboardStream


class CoursesStream(blackboardStream):
    """Stream for Learn courses.

    The courses list endpoint accepts ``modified`` filters but does not return
    a ``modified`` field on records, so this stream is full-table. ``start_date``
    is still applied as an API-side filter when configured.
    """

    name = "courses"
    path = "/v1/courses"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Course primary key"),
        th.Property("uuid", th.StringType),
        th.Property("externalId", th.StringType),
        th.Property("dataSourceId", th.StringType),
        th.Property("courseId", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("modified", th.DateTimeType),
        th.Property("organization", th.BooleanType),
        th.Property("ultraEnabled", th.BooleanType),
        th.Property("allowGuests", th.BooleanType),
        th.Property("closedComplete", th.BooleanType),
        th.Property("termId", th.StringType),
        th.Property("availability", th.ObjectType()),
        th.Property("enrollment", th.ObjectType()),
        th.Property("locale", th.ObjectType()),
        th.Property("externalAccessUrl", th.StringType),
        th.Property("guestAccessUrl", th.StringType),
        th.Property("hasChildren", th.BooleanType),
        th.Property("parentId", th.StringType),
    ).to_dict()

    @override
    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Pass the course id down to course-scoped child streams."""
        return {"course_id": record["id"]}

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Apply ``start_date`` as a modified filter even without a replication key."""
        params = super().get_url_params(context, next_page_token)
        start_date = self.get_starting_time(context)
        if start_date and "modified" not in params:
            params["modified"] = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["modifiedCompare"] = "greaterOrEqual"
        return params

    @override
    def get_records(self, context: dict | None):
        """Optionally fetch only configured ``course_ids`` (by id) instead of the full list."""
        allowed = list(self.config.get("course_ids") or [])
        if not allowed:
            yield from super().get_records(context)
            return

        for course_id in allowed:
            prepared = self.build_prepared_request(
                method=self.rest_method,
                url=f"{self.url_base}/v1/courses/{course_id}",
                params={},
                headers=self.http_headers,
                json=None,
            )
            resp = self._request(prepared, context)
            yield resp.json()


class EnrollmentsStream(blackboardChildStream):
    """Stream for course memberships (enrollments)."""

    name = "enrollments"
    path = "/v1/courses/{course_id}/users"
    parent_stream_type = CoursesStream
    ignore_parent_replication_key = True
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "modified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Membership primary key"),
        th.Property("userId", th.StringType),
        th.Property("courseId", th.StringType),
        th.Property("course_id", th.StringType, description="Parent course id"),
        th.Property("dataSourceId", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("modified", th.DateTimeType),
        th.Property("availability", th.ObjectType()),
        th.Property("courseRoleId", th.StringType),
        th.Property("lastAccessed", th.DateTimeType),
        th.Property("childCourseId", th.StringType),
    ).to_dict()

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Stamp the parent course id onto the record."""
        if context:
            row["course_id"] = context["course_id"]
        return row


class GradebookColumnsStream(blackboardChildStream):
    """Stream for gradebook columns (assignments and exams)."""

    name = "gradebook_columns"
    path = "/v2/courses/{course_id}/gradebook/columns"
    parent_stream_type = CoursesStream
    ignore_parent_replication_key = True
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = "modified"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Column primary key"),
        th.Property("name", th.StringType),
        th.Property("displayName", th.StringType),
        th.Property("externalId", th.StringType),
        th.Property("description", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("modified", th.DateTimeType),
        th.Property("course_id", th.StringType, description="Parent course id"),
        th.Property("externalGrade", th.BooleanType),
        th.Property("score", th.ObjectType()),
        th.Property("availability", th.ObjectType()),
        th.Property("grading", th.ObjectType()),
        th.Property("gradebookCategoryId", th.StringType),
        th.Property("contentId", th.StringType),
        th.Property("scoreProviderHandle", th.StringType),
    ).to_dict()

    @override
    def get_child_context(self, record: dict, context: dict | None) -> dict:
        """Pass course and column ids down to the grades stream."""
        return {
            "course_id": (context or {}).get("course_id") or record.get("courseId"),
            "column_id": record["id"],
        }

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Stamp the parent course id onto the record."""
        if context:
            row["course_id"] = context["course_id"]
        return row


class GradesStream(blackboardChildStream):
    """Stream for per-user grades on a gradebook column.

    Grade payloads are inconsistent about a ``modified`` timestamp, so this
    stream is full-table within each course/column partition.
    """

    name = "grades"
    path = "/v2/courses/{course_id}/gradebook/columns/{column_id}/users"
    parent_stream_type = GradebookColumnsStream
    ignore_parent_replication_key = True
    primary_keys: ClassVar[list[str]] = ["userId", "columnId"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("userId", th.StringType),
        th.Property("columnId", th.StringType),
        th.Property("course_id", th.StringType, description="Parent course id"),
        th.Property("column_id", th.StringType, description="Parent column id"),
        th.Property("status", th.StringType),
        th.Property("displayGrade", th.ObjectType()),
        th.Property("text", th.StringType),
        th.Property("score", th.NumberType),
        th.Property("possible", th.NumberType),
        th.Property("percent", th.NumberType),
        th.Property("feedback", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("exempt", th.BooleanType),
        th.Property("corrupt", th.BooleanType),
        th.Property("gradeNotationId", th.StringType),
        th.Property("changeIndex", th.IntegerType),
        th.Property("created", th.DateTimeType),
        th.Property("modified", th.DateTimeType),
        th.Property("overridden", th.DateTimeType),
        th.Property("firstAttempted", th.DateTimeType),
        th.Property("lastAttempted", th.DateTimeType),
    ).to_dict()

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Stamp parent course and column ids onto the record."""
        if context:
            row["course_id"] = context["course_id"]
            row.setdefault("columnId", context["column_id"])
            row["column_id"] = context["column_id"]
        return row


class AnnouncementsStream(blackboardStream):
    """Stream for system-level announcements.

    Like courses, the list endpoint filters by ``modified`` but does not return
    that field on records, so sync is full-table with an optional start_date filter.
    """

    name = "announcements"
    path = "/v1/announcements"
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Apply ``start_date`` as a modified filter even without a replication key."""
        params = super().get_url_params(context, next_page_token)
        start_date = self.get_starting_time(context)
        if start_date and "modified" not in params:
            params["modified"] = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["modifiedCompare"] = "greaterOrEqual"
        return params

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Announcement primary key"),
        th.Property("title", th.StringType),
        th.Property("body", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("modified", th.DateTimeType),
        th.Property("showStart", th.DateTimeType),
        th.Property("showEnd", th.DateTimeType),
        th.Property("availability", th.ObjectType()),
    ).to_dict()


class CourseAnnouncementsStream(blackboardChildStream):
    """Stream for course-scoped announcements."""

    name = "course_announcements"
    path = "/v1/courses/{course_id}/announcements"
    parent_stream_type = CoursesStream
    ignore_parent_replication_key = True
    primary_keys: ClassVar[list[str]] = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Announcement primary key"),
        th.Property("title", th.StringType),
        th.Property("body", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("modified", th.DateTimeType),
        th.Property("showStart", th.DateTimeType),
        th.Property("showEnd", th.DateTimeType),
        th.Property("course_id", th.StringType, description="Parent course id"),
        th.Property("availability", th.ObjectType()),
    ).to_dict()

    @override
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Stamp the parent course id onto the record."""
        if context:
            row["course_id"] = context["course_id"]
        return row
