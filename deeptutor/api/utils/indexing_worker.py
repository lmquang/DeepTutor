"""Subprocess adapters for memory-heavy knowledge indexing jobs."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import tempfile
import threading
from typing import Any

from deeptutor.runtime.isolated_worker import run_in_isolated_process

_CHILD_ENTRY = "deeptutor.api.utils.indexing_worker:_run_indexing_child"
_MAX_EVENT_LINE = 2 * 1024 * 1024


def _timeout() -> float:
    raw = os.environ.get("DEEPTUTOR_INDEXING_TIMEOUT_SECONDS", "21600")
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "DEEPTUTOR_INDEXING_TIMEOUT_SECONDS must be a finite positive number"
        ) from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError("DEEPTUTOR_INDEXING_TIMEOUT_SECONDS must be a finite positive number")
    return value


def _append_event(path: str, task_id: str, event: str, payload: dict[str, Any]) -> None:
    record = json.dumps(
        {"task_id": task_id, "event": event, "payload": payload},
        ensure_ascii=False,
        default=str,
    )
    encoded = record.encode("utf-8")
    if len(encoded) > _MAX_EVENT_LINE:
        record = json.dumps(
            {
                "task_id": task_id,
                "event": event,
                "payload": {"detail": str(payload.get("detail", ""))[:8192], "truncated": True},
            },
            ensure_ascii=False,
        )
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(record + "\n")
        handle.flush()


def _run_indexing_child(kind: str, payload: dict[str, Any], event_path: str) -> None:
    """Importable synchronous subprocess entry point."""
    from deeptutor.api.utils.task_log_stream import get_task_stream_manager
    from deeptutor.knowledge.progress_events import install_progress_ports

    task_id = str(payload["task_id"])
    manager = get_task_stream_manager()
    event_lock = threading.Lock()

    def emit(emitted_task_id: str, event: str, body: dict[str, Any]) -> None:
        # Parsing/indexing callbacks may emit from several threads.
        with event_lock:
            _append_event(event_path, emitted_task_id, event, body)

    manager.emit = emit

    async def no_broadcast(_kb_name: str, _progress: dict[str, Any]) -> None:
        return None

    install_progress_ports(broadcast=no_broadcast, emit_task_event=manager.emit)

    async def invoke() -> None:
        from deeptutor.api.routers.knowledge import (
            run_initialization_task,
            run_reindex_task,
            run_upload_processing_task,
        )

        kwargs = dict(payload)
        kwargs.pop("task_id", None)
        if kind == "initialization":
            from deeptutor.knowledge.initializer import KnowledgeBaseInitializer

            initializer = KnowledgeBaseInitializer(**kwargs.pop("initializer"))
            kwargs.pop("kb_name", None)
            kwargs.pop("base_dir", None)
            await run_initialization_task(initializer, task_id, **kwargs)
        elif kind == "upload":
            await run_upload_processing_task(task_id=task_id, **kwargs)
        elif kind == "reindex":
            await run_reindex_task(task_id=task_id, **kwargs)
        else:
            raise ValueError(f"Unknown indexing worker operation: {kind}")

    asyncio.run(invoke())


def _read_new_events(
    path: Path, offset: int, remainder: bytes
) -> tuple[int, bytes, list[dict[str, Any]]]:
    if not path.exists():
        return offset, remainder, []
    with path.open("rb") as handle:
        handle.seek(offset)
        chunk = handle.read(256 * 1024)
        new_offset = handle.tell()
    data = remainder + chunk
    lines = data.split(b"\n")
    remainder = lines.pop()
    if len(remainder) > _MAX_EVENT_LINE:
        remainder = b""
    events = []
    for line in lines:
        if not line or len(line) > _MAX_EVENT_LINE:
            continue
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                events.append(value)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    return new_offset, remainder, events


def _replay(events: list[dict[str, Any]], expected_task_id: str) -> bool:
    from deeptutor.api.utils.task_id_manager import TaskIDManager
    from deeptutor.api.utils.task_log_stream import get_task_stream_manager

    terminal = False
    stream = get_task_stream_manager()
    tasks = TaskIDManager.get_instance()
    for item in events:
        if item.get("task_id") != expected_task_id:
            continue
        event = str(item.get("event") or "")
        payload = item.get("payload")
        if not event or not isinstance(payload, dict):
            continue
        stream.emit(expected_task_id, event, payload)
        if event == "complete":
            tasks.update_task_status(expected_task_id, "completed")
            terminal = True
        elif event == "failed":
            tasks.update_task_status(expected_task_id, "error", error=payload.get("detail"))
            terminal = True
    return terminal


async def _broadcast_events(events: list[dict[str, Any]], task_id: str) -> None:
    from deeptutor.knowledge.progress_events import broadcast_progress

    for item in events:
        if item.get("task_id") == task_id and item.get("event") == "progress":
            progress = item.get("payload")
            if isinstance(progress, dict) and progress.get("kb_name"):
                await broadcast_progress(str(progress["kb_name"]), progress)


def _invalidate_index_caches() -> None:
    from deeptutor.services.rag.pipelines.llamaindex.storage import clear_index_cache
    from deeptutor.services.rag.pipelines.llamaindex.retrievers import _scoped_index

    clear_index_cache()
    _scoped_index.cache_clear()


def _report_failure(payload: dict[str, Any], error: str) -> None:
    """Persist failures in the admitted storage scope, including shutdown."""
    from contextlib import ExitStack

    from deeptutor.api.utils.task_id_manager import TaskIDManager
    from deeptutor.api.utils.task_log_stream import get_task_stream_manager
    from deeptutor.knowledge.progress_tracker import ProgressStage, ProgressTracker
    from deeptutor.multi_user.context import reset_current_user, set_current_user
    from deeptutor.services.workspace.context import workspace_context
    from deeptutor.services.workspace.knowledge import library_request

    task_id = str(payload["task_id"])
    owner = payload.get("owner") or payload.get("initializer", {}).get("owner")
    with ExitStack() as stack:
        if owner is not None:
            token = set_current_user(owner)
            stack.callback(reset_current_user, token)
        if payload.get("storage_workspace_id") is not None:
            stack.enter_context(workspace_context(payload["storage_workspace_id"]))
            token = library_request.set(False)
            stack.callback(library_request.reset, token)
        try:
            tracker = ProgressTracker(str(payload["kb_name"]), Path(str(payload["base_dir"])))
            tracker.task_id = task_id
            tracker.update(ProgressStage.ERROR, error=error, message=f"Indexing failed: {error}")
        finally:
            TaskIDManager.get_instance().update_task_status(task_id, "error", error=error)
            get_task_stream_manager().emit_failed(task_id, error)


@contextmanager
def _event_directory(backend: str):
    if backend == "kubernetes":
        # Shared with the worker pod. Retain IPC artifacts on failures so an
        # unconfirmed pod termination can never race directory removal.
        root = Path(os.environ.get("DEEPTUTOR_INDEXING_JOB_ROOT", "/app/data/indexing-jobs"))
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory = tempfile.mkdtemp(prefix="index-", dir=root)
        yield directory
    else:
        with tempfile.TemporaryDirectory(prefix="deeptutor-index-events-") as directory:
            yield directory


async def _execute_worker(
    backend: str, kind: str, payload: dict[str, Any], event_path: Path, timeout: float
) -> None:
    if backend == "kubernetes":
        from deeptutor.api.utils.indexing_kubernetes import run_in_kubernetes_job

        await run_in_kubernetes_job(kind, payload, str(event_path), timeout=timeout)
    else:
        await run_in_isolated_process(_CHILD_ENTRY, kind, payload, str(event_path), timeout=timeout)


async def _run(kind: str, payload: dict[str, Any]) -> None:
    task_id = str(payload["task_id"])
    terminal = False
    backend = os.environ.get("DEEPTUTOR_INDEXING_BACKEND", "subprocess")
    try:
        if backend not in {"subprocess", "kubernetes"}:
            raise ValueError(f"Unsupported indexing backend: {backend}")
        timeout = _timeout()
    except ValueError as exc:
        _report_failure(payload, str(exc))
        return
    with _event_directory(backend) as directory:
        event_path = Path(directory) / "events.jsonl"
        worker = asyncio.create_task(_execute_worker(backend, kind, payload, event_path, timeout))
        offset, remainder = 0, b""
        try:
            while not worker.done():
                offset, remainder, events = await asyncio.to_thread(
                    _read_new_events, event_path, offset, remainder
                )
                terminal = _replay(events, task_id) or terminal
                await _broadcast_events(events, task_id)
                await asyncio.sleep(0.05)
            await worker
            # Drain all chunks: a fast child can finish before the first poll.
            while True:
                previous_offset = offset
                offset, remainder, events = await asyncio.to_thread(
                    _read_new_events, event_path, offset, remainder
                )
                terminal = _replay(events, task_id) or terminal
                await _broadcast_events(events, task_id)
                if offset == previous_offset:
                    break
            if not terminal:
                raise RuntimeError("Indexing worker exited without a terminal event")
            if backend == "kubernetes":
                import shutil

                await asyncio.to_thread(shutil.rmtree, directory)
        except asyncio.CancelledError:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            if not terminal:
                _report_failure(payload, "Indexing interrupted: API task was cancelled")
            raise
        except Exception as exc:
            # Relay failures must also stop/reap the child before deleting IPC files.
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            if not terminal:
                _report_failure(payload, str(exc))
        finally:
            _invalidate_index_caches()


async def run_upload_processing_isolated(
    kb_name: str, base_dir: str, uploaded_file_paths: list[str], task_id: str, **kwargs: Any
) -> None:
    from deeptutor.multi_user.context import get_current_user_or_none

    kwargs.setdefault("owner", get_current_user_or_none())
    await _run(
        "upload",
        {
            "kb_name": kb_name,
            "base_dir": str(Path(base_dir).resolve()),
            "uploaded_file_paths": list(uploaded_file_paths),
            "task_id": task_id,
            **kwargs,
        },
    )


async def run_reindex_isolated(
    kb_name: str, base_dir: str, task_id: str, signature_hash: str, **kwargs: Any
) -> None:
    from deeptutor.multi_user.context import get_current_user_or_none

    kwargs.setdefault("owner", get_current_user_or_none())
    await _run(
        "reindex",
        {
            "kb_name": kb_name,
            "base_dir": str(Path(base_dir).resolve()),
            "task_id": task_id,
            "signature_hash": signature_hash,
            **kwargs,
        },
    )


async def run_initialization_isolated(
    initializer: Any, task_id: str, *, storage_workspace_id: str | None = None
) -> None:
    from deeptutor.multi_user.context import get_current_user_or_none

    dto = {
        name: getattr(initializer, name, None)
        for name in ("kb_name", "api_key", "base_url", "rag_provider", "indexing_snapshot", "owner")
    }
    dto["base_dir"] = str(Path(initializer.base_dir).resolve())
    if dto["owner"] is None:
        dto["owner"] = get_current_user_or_none()
    await _run(
        "initialization",
        {
            "kb_name": dto["kb_name"],
            "base_dir": dto["base_dir"],
            "task_id": task_id,
            "initializer": dto,
            "storage_workspace_id": storage_workspace_id,
        },
    )


__all__ = ["run_initialization_isolated", "run_reindex_isolated", "run_upload_processing_isolated"]
