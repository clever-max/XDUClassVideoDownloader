"""Core orchestration for single-course and batch operations."""

import configparser
from copy import deepcopy
from typing import Any, Dict, Iterable, Mapping, Optional

from .auth import get_auth_cookies
from .courses import default_term
from .downloader import download_course_videos, process_all_courses
from .models import CourseDownloadOptions
from .platform import scan_courses
from .platform import check_update as _platform_check_update
from .utils import parse_date_ranges, parse_lesson_ranges, parse_week_ranges
from .validator import validate_download_parameters


def _resolve_auth(auth_cookies: Optional[Mapping[str, str]]) -> Dict[str, str]:
    return dict(auth_cookies) if auth_cookies is not None else get_auth_cookies()


def check_update() -> bool:
    return _platform_check_update()


def scan_term_courses(
    user_id: str,
    term_year: int,
    term_id: int,
    *,
    auth_cookies: Optional[Mapping[str, str]] = None,
) -> Dict[int, Dict[str, Any]]:
    return scan_courses(
        user_id,
        term_year,
        term_id,
        auth_cookies=_resolve_auth(auth_cookies),
    )


def download_course(
    options: CourseDownloadOptions,
    *,
    auth_cookies: Optional[Mapping[str, str]] = None,
) -> bool:
    validated_live_id, validated_single, validated_video_type = validate_download_parameters(
        options.live_id, options.single, options.video_type
    )
    skip_weeks = parse_week_ranges(options.skip_weeks)
    weeks = parse_week_ranges(options.weeks)
    dates = parse_date_ranges(options.dates)
    lessons = parse_lesson_ranges(options.lessons)
    filters = {}
    if weeks:
        filters["include_weeks"] = weeks
    if dates:
        filters["dates"] = dates
    if lessons:
        filters["lesson_numbers"] = lessons
    return download_course_videos(
        validated_live_id,
        validated_single,
        options.merge,
        validated_video_type,
        skip_weeks,
        **filters,
        auth_cookies=_resolve_auth(auth_cookies),
    )


def download_courses(
    config: configparser.ConfigParser,
    *,
    selected_sections: Optional[Iterable[str]] = None,
    video_type: Optional[str] = None,
    weeks: str = "",
    dates: str = "",
    lessons: str = "",
    course_filters: Optional[Mapping[str, Mapping[str, str]]] = None,
    auth_cookies: Optional[Mapping[str, str]] = None,
) -> bool:
    """Download selected courses from an in-memory copy of automation config."""

    runtime_config = deepcopy(config)
    if selected_sections is not None:
        selected = {str(section) for section in selected_sections}
        for section in runtime_config.sections():
            runtime_config[section]["download"] = "yes" if section in selected else "no"

    resolved_video_type = video_type or runtime_config["DEFAULT"].get("video_type", "both")
    filters = {}
    parsed_weeks = parse_week_ranges(weeks)
    parsed_dates = parse_date_ranges(dates)
    parsed_lessons = parse_lesson_ranges(lessons)
    if parsed_weeks:
        filters["include_weeks"] = parsed_weeks
    if parsed_dates:
        filters["dates"] = parsed_dates
    if parsed_lessons:
        filters["lesson_numbers"] = parsed_lessons
    normalized_course_filters = {}
    for section, raw_filter in (course_filters or {}).items():
        normalized_course_filters[str(section)] = {
            "include_weeks": parse_week_ranges(str(raw_filter.get("weeks", ""))),
            "dates": parse_date_ranges(str(raw_filter.get("dates", ""))),
            "lesson_numbers": parse_lesson_ranges(str(raw_filter.get("lessons", ""))),
        }
    if normalized_course_filters:
        filters["course_filters"] = normalized_course_filters
    return process_all_courses(
        runtime_config,
        resolved_video_type,
        auth_cookies=_resolve_auth(auth_cookies),
        **filters,
    )


__all__ = [
    "CourseDownloadOptions",
    "check_update",
    "default_term",
    "download_course",
    "download_courses",
    "scan_term_courses",
]
