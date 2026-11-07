"""Process boundary and live-event regression tests for KB indexing."""

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from deeptutor.api.utils import indexing_worker as worker
from deeptutor.runtime.isolated_worker import IsolatedWorkerError, run_in_isolated_process
from deeptutor.runtime.worker_process import _resolve


def test_child_entry_is_importable():
    assert _resolve(worker._CHILD_ENTRY) is worker._run_indexing_child


@pytest.mark.parametrize("value", ["nan", "inf", "-1", "0", "invalid"])
def test_timeout_rejects_invalid_values(monkeypatch, value):
    monkeypatch.setenv("DEEPTUTOR_INDEXING_TIMEOUT_SECONDS", value)
    with pytest.raises(ValueError, match="finite positive"):
        worker._timeout()


@pytest.mark.asyncio
async def test_payload_preserves_scope_and_snapshot(monkeypatch, tmp_path):
    captured = []

    async def record(kind, payload):
        captured.append((kind, payload))

    monkeypatch.setattr(worker, "_run", record)
    snapshot = {"accepted": "snapshot"}
    await worker.run_reindex_isolated(
        "kb",
        str(tmp_path),
        "task",
        "signature",
        owner="owner",
        indexing_snapshot=snapshot,
        storage_workspace_id="workspace",
    )
    kind, payload = captured[0]
    assert kind == "reindex"
    assert payload["owner"] == "owner"
    assert payload["storage_workspace_id"] == "workspace"
    assert payload["indexing_snapshot"] is snapshot
    assert Path(payload["base_dir"]).is_absolute()

    initializer = SimpleNamespace(
        kb_name="kb",
        base_dir=tmp_path,
        owner="owner",
        rag_provider="llamaindex",
        indexing_snapshot=snapshot,
        api_key=None,
        base_url=None,
    )
    await worker.run_initialization_isolated(initializer, "init", storage_workspace_id="ws")
    kind, payload = captured[1]
    assert kind == "initialization"
    assert payload["initializer"]["owner"] == "owner"
    assert payload["initializer"]["indexing_snapshot"] is snapshot
    assert "progress_tracker" not in payload["initializer"]


@pytest.mark.asyncio
async def test_fast_child_drains_more_than_one_event_chunk(monkeypatch):
    replayed = []

    async def child(entry, kind, payload, event_path, timeout):
        assert entry == worker._CHILD_ENTRY
        for _ in range(90):
            worker._append_event(event_path, "task", "progress", {"message": "x" * 4096})
        worker._append_event(event_path, "task", "complete", {"detail": "done"})

    def replay(events, task_id):
        replayed.extend(events)
        return any(e["event"] == "complete" for e in events)

    monkeypatch.setattr(worker, "run_in_isolated_process", child)
    monkeypatch.setattr(worker, "_replay", replay)
    monkeypatch.setattr(worker, "_invalidate_index_caches", lambda: None)
    await worker._run("upload", {"task_id": "task", "kb_name": "kb", "base_dir": "."})
    assert len(replayed) == 91
    assert replayed[-1]["event"] == "complete"


@pytest.mark.asyncio
async def test_kubernetes_events_relay_overall_counts_before_terminal(monkeypatch, tmp_path):
    monkeypatch.setenv("DEEPTUTOR_INDEXING_BACKEND", "kubernetes")
    monkeypatch.setenv("DEEPTUTOR_INDEXING_JOB_ROOT", str(tmp_path))
    replayed, broadcasts, failures = [], [], []
    paths = []

    async def execute(backend, kind, payload, path, timeout):
        assert backend == "kubernetes"
        assert path.parent.parent == tmp_path
        paths.append(path)
        worker._append_event(
            str(path),
            "task",
            "progress",
            {
                "kb_name": "kb",
                "task_id": "task",
                "stage": "processing_file",
                "current": 64,
                "total": 20203,
                "message": "Indexed 64 confirmed file(s)",
            },
        )
        worker._append_event(str(path), "task", "complete", {"detail": "done"})

    def replay(events, task_id):
        replayed.extend(events)
        return any(item["event"] == "complete" for item in events)

    async def broadcast(events, task_id):
        broadcasts.extend(item for item in events if item["event"] == "progress")

    monkeypatch.setattr(worker, "_execute_worker", execute)
    monkeypatch.setattr(worker, "_replay", replay)
    monkeypatch.setattr(worker, "_broadcast_events", broadcast)
    monkeypatch.setattr(worker, "_report_failure", lambda *args: failures.append(args))
    monkeypatch.setattr(worker, "_invalidate_index_caches", lambda: None)
    await worker._run("upload", {"task_id": "task"})
    assert [item["event"] for item in replayed] == ["progress", "complete"]
    assert broadcasts[0]["payload"]["current"] == 64
    assert broadcasts[0]["payload"]["total"] == 20203
    assert broadcasts[0]["payload"]["stage"] == "processing_file"
    assert not paths[0].parent.exists()
    assert failures == []


def test_event_reader_retains_partial_json(tmp_path):
    path = tmp_path / "events"
    data = json.dumps({"task_id": "task", "event": "progress", "payload": {}}).encode()
    path.write_bytes(data[:15])
    offset, remainder, events = worker._read_new_events(path, 0, b"")
    assert events == []
    with path.open("ab") as handle:
        handle.write(data[15:] + b"\n")
    _, remainder, events = worker._read_new_events(path, offset, remainder)
    assert remainder == b""
    assert events[0]["event"] == "progress"


@pytest.mark.asyncio
async def test_cancellation_waits_for_child_cleanup(monkeypatch):
    started = asyncio.Event()
    stopped = asyncio.Event()
    failures = []
    monkeypatch.setattr(worker, "_report_failure", lambda payload, error: failures.append(error))

    async def child(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(worker, "run_in_isolated_process", child)
    monkeypatch.setattr(worker, "_invalidate_index_caches", lambda: None)
    task = asyncio.create_task(worker._run("upload", {"task_id": "task"}))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stopped.is_set()
    assert "interrupted" in failures[0]


@pytest.mark.asyncio
async def test_worker_crash_reports_failure(monkeypatch):
    failures = []

    async def crash(*args, **kwargs):
        raise IsolatedWorkerError("worker crashed")

    monkeypatch.setattr(worker, "run_in_isolated_process", crash)
    monkeypatch.setattr(worker, "_report_failure", lambda payload, error: failures.append(error))
    monkeypatch.setattr(worker, "_invalidate_index_caches", lambda: None)
    await worker._run("upload", {"task_id": "task"})
    assert failures == ["worker crashed"]


@pytest.mark.asyncio
async def test_real_subprocess_has_different_pid_and_keeps_loop_responsive():
    child_pid = await run_in_isolated_process("os:getpid", timeout=10)
    assert child_pid != os.getpid()
    sleeper = asyncio.create_task(run_in_isolated_process("time:sleep", 0.5, timeout=10))
    ticks = 0
    while not sleeper.done():
        await asyncio.sleep(0.01)
        ticks += 1
    await sleeper
    assert ticks >= 10
