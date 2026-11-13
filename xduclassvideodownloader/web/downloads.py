"""Background download jobs used by the WebUI."""

import io
import logging
import sys
import threading
import time
import uuid
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple

from ..core import download_course, download_courses
from ..core.config import AUTOMATION_CONFIG_FILE, safe_read_config
from ..core.downloader import combine_video_views
from ..core.models import CourseDownloadOptions
from ..core.utils import setup_logging


logger = setup_logging("webui")
DOWNLOAD_ACTIVE_STATUSES = {"pending", "running"}


class QueueWriter(io.TextIOBase):
    def __init__(self, emit: Callable[[str], None]) -> None:
        self.emit = emit

    def writable(self) -> bool:
        return True

    def write(self, value: str) -> int:
        if not value:
            return 0
        self.emit(str(value))
        return len(value)

    def flush(self) -> None:
        return None


class WritableTextStream(Protocol):
    def write(self, text: str, /) -> int:
        ...

    def flush(self) -> None:
        ...


class ThreadBoundStream(io.TextIOBase):
    def __init__(self, target_thread_id: int, target: WritableTextStream, fallback: WritableTextStream) -> None:
        self.target_thread_id = target_thread_id
        self.target = target
        self.fallback = fallback

    def _stream(self) -> WritableTextStream:
        return self.target if threading.get_ident() == self.target_thread_id else self.fallback

    def writable(self) -> bool:
        return True

    def write(self, value: str) -> int:
        return self._stream().write(value)

    def flush(self) -> None:
        self._stream().flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.fallback, name)


class QueueLogHandler(logging.Handler):
    def __init__(self, emit: Callable[[str], None]) -> None:
        super().__init__(logging.INFO)
        self.emit_output = emit
        self.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.emit_output(self.format(record) + "\n")
        except Exception:
            pass


class DownloadJob:
    def __init__(self, target: Any, args: Tuple[Any, ...]) -> None:
        self.id = uuid.uuid4().hex
        self.target = target
        self.args = args
        self.history: List[str] = []
        self.history_start = 0
        self.output_count = 0
        self.condition = threading.Condition()
        self.status = "pending"
        self.state_version = 0
        self.success: Optional[bool] = None
        self.error: Optional[str] = None
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self.thread = threading.Thread(target=self._run, name=f"webui-download-{self.id}", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def emit_output(self, text: str) -> None:
        with self.condition:
            self.history.append(text)
            self.output_count += 1
            if len(self.history) > 5000:
                self.history_start += len(self.history) - 5000
                self.history = self.history[-5000:]
            self.condition.notify_all()

    def set_status(
        self,
        status: str,
        *,
        success: Optional[bool] = None,
        error: Optional[str] = None,
    ) -> None:
        with self.condition:
            self.status = status
            if success is not None:
                self.success = success
            if error is not None:
                self.error = error
            self.state_version += 1
            self.condition.notify_all()

    def snapshot(self) -> Dict[str, Any]:
        with self.condition:
            return {
                "id": self.id,
                "status": self.status,
                "success": self.success,
                "error": self.error,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "state_version": self.state_version,
                "output_cursor": self.output_count - 1,
            }

    def _run(self) -> None:
        with self.condition:
            self.started_at = time.time()
        self.set_status("running")
        writer = QueueWriter(self.emit_output)
        handler = QueueLogHandler(self.emit_output)
        root = logging.getLogger("xdu")
        root.addHandler(handler)
        stdout_proxy = ThreadBoundStream(threading.get_ident(), writer, sys.stdout)
        stderr_proxy = ThreadBoundStream(threading.get_ident(), writer, sys.stderr)
        original_stdout, original_stderr = sys.stdout, sys.stderr
        try:
            sys.stdout = stdout_proxy
            sys.stderr = stderr_proxy
            success = bool(self.target(*self.args))
            self.set_status("success" if success else "failed", success=success)
        except Exception as exc:
            self.emit_output(f"ERROR: {exc}\n")
            self.set_status("failed", success=False, error=str(exc))
            logger.exception("WebUI download job failed")
        finally:
            if sys.stdout is stdout_proxy:
                sys.stdout = original_stdout
            if sys.stderr is stderr_proxy:
                sys.stderr = original_stderr
            writer.flush()
            root.removeHandler(handler)
            with self.condition:
                self.finished_at = time.time()
                self.state_version += 1
                self.condition.notify_all()


class DownloadJobManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: Dict[str, DownloadJob] = {}
        self._active_job_id: Optional[str] = None

    def start(self, target: Any, args: Tuple[Any, ...]) -> DownloadJob:
        with self._lock:
            if self._active_job_id:
                active = self._jobs.get(self._active_job_id)
                if active and active.status in DOWNLOAD_ACTIVE_STATUSES:
                    raise RuntimeError("已有下载任务正在运行")
                self._active_job_id = None
            job = DownloadJob(target, args)
            self._jobs[job.id] = job
            self._active_job_id = job.id
            job.start()
            return job

    def get(self, job_id: str) -> Optional[DownloadJob]:
        return self._jobs.get(job_id)

    def active(self) -> Optional[DownloadJob]:
        with self._lock:
            if not self._active_job_id:
                return None
            job = self._jobs.get(self._active_job_id)
            if job and job.status in DOWNLOAD_ACTIVE_STATUSES:
                return job
            return None


def run_single_download(
    live_id: str,
    single: int,
    merge: bool,
    video_type: str,
    skip_weeks_text: str,
    weeks_text: str = "",
    dates_text: str = "",
    lessons_text: str = "",
) -> bool:
    return download_course(
        CourseDownloadOptions(
            live_id=live_id,
            single=single,
            merge=merge,
            video_type=video_type,
            skip_weeks=skip_weeks_text,
            weeks=weeks_text,
            dates=dates_text,
            lessons=lessons_text,
        )
    )


def run_batch_download(
    selected_sections: Iterable[str],
    video_type: str,
    weeks_text: str = "",
    dates_text: str = "",
    lessons_text: str = "",
    course_filters: Optional[Mapping[str, Mapping[str, str]]] = None,
) -> bool:
    config = safe_read_config(AUTOMATION_CONFIG_FILE)
    selected = {str(section) for section in selected_sections}
    filters = {}
    if weeks_text:
        filters["weeks"] = weeks_text
    if dates_text:
        filters["dates"] = dates_text
    if lessons_text:
        filters["lessons"] = lessons_text
    if course_filters:
        filters["course_filters"] = course_filters
    return download_courses(
        config,
        selected_sections=selected,
        video_type=video_type,
        **filters,
    )


def run_combined_export(ppt_file: str, teacher_file: str, output_file: str, ppt_offset: float = 0.0) -> bool:
    return combine_video_views(ppt_file, teacher_file, output_file, ppt_offset=ppt_offset)


jobs = DownloadJobManager()


__all__ = [
    "DOWNLOAD_ACTIVE_STATUSES",
    "DownloadJob",
    "DownloadJobManager",
    "jobs",
    "run_batch_download",
    "run_combined_export",
    "run_single_download",
]
