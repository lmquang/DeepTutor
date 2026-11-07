import asyncio
from pathlib import Path
import pickle

import pytest

from deeptutor.api.utils import indexing_kubernetes as kubernetes


def _template():
    return {"spec": {"template": {"spec": {"containers": [{"name": "worker"}]}}}}


@pytest.mark.asyncio
async def test_job_uses_worker_protocol_and_template(monkeypatch, tmp_path):
    events = tmp_path / "events.jsonl"
    calls = []

    monkeypatch.setattr(kubernetes, "_namespace", lambda: "testing")
    monkeypatch.setattr(kubernetes, "_template", _template)

    def request(method, path, body=None):
        calls.append((method, path, body))
        if method == "POST":
            request_path = Path(body["spec"]["template"]["spec"]["containers"][0]["args"][0])
            protocol = pickle.loads(request_path.read_bytes())
            assert protocol["callable_path"] == kubernetes._CHILD_ENTRY
            assert protocol["args"] == ("upload", {"task_id": "t"}, str(events.resolve()))
            Path(body["spec"]["template"]["spec"]["containers"][0]["args"][1]).write_bytes(
                pickle.dumps({"ok": True, "result": "done"})
            )
            return {}
        if method == "GET" and len([c for c in calls if c[0] == "GET"]) == 1:
            return {"status": {"succeeded": 1}}
        raise kubernetes.KubernetesJobError("Kubernetes API GET x failed (404): gone")

    monkeypatch.setattr(kubernetes, "_request", request)
    assert await kubernetes.run_in_kubernetes_job("upload", {"task_id": "t"}, events, 10) == "done"
    posted = calls[0][2]
    assert posted["metadata"]["namespace"] == "testing"
    assert posted["metadata"]["name"].startswith("deeptutor-index-")
    assert posted["spec"]["backoffLimit"] == 0
    assert posted["spec"]["activeDeadlineSeconds"] == 10
    assert (tmp_path / "request.pickle").stat().st_mode & 0o777 == 0o600
    assert any(call[0] == "DELETE" and "propagationPolicy=Foreground" in call[1] for call in calls)


@pytest.mark.asyncio
async def test_cancellation_waits_for_foreground_delete(monkeypatch, tmp_path):
    deleted = asyncio.Event()
    monkeypatch.setattr(kubernetes, "_namespace", lambda: "testing")
    monkeypatch.setattr(kubernetes, "_template", _template)
    monkeypatch.setattr(kubernetes, "_request", lambda *args, **kwargs: {})

    async def delete(namespace, name):
        await asyncio.sleep(0)
        deleted.set()

    monkeypatch.setattr(kubernetes, "_delete", delete)
    task = asyncio.create_task(
        kubernetes.run_in_kubernetes_job("reindex", {}, tmp_path / "events", 10)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert deleted.is_set()


@pytest.mark.asyncio
async def test_rejects_unknown_operation_before_api_call(tmp_path):
    with pytest.raises(ValueError, match="Unknown"):
        await kubernetes.run_in_kubernetes_job("evil", {}, tmp_path / "events", 1)


def test_namespace_prefers_indexing_environment(monkeypatch):
    monkeypatch.setenv("DEEPTUTOR_INDEXING_NAMESPACE", "jobs")
    assert kubernetes._namespace() == "jobs"


@pytest.mark.asyncio
async def test_failed_post_still_deletes_known_job_name(monkeypatch, tmp_path):
    deleted = []
    monkeypatch.setattr(kubernetes, "_namespace", lambda: "testing")
    monkeypatch.setattr(kubernetes, "_template", _template)

    def request(method, path, body=None):
        if method == "POST":
            raise kubernetes.KubernetesJobError("ambiguous create failure")
        return {}

    async def delete(namespace, name):
        deleted.append((namespace, name))

    monkeypatch.setattr(kubernetes, "_request", request)
    monkeypatch.setattr(kubernetes, "_delete", delete)
    with pytest.raises(kubernetes.KubernetesJobError, match="ambiguous"):
        await kubernetes.run_in_kubernetes_job("upload", {}, tmp_path / "events", 10)
    assert deleted and deleted[0][0] == "testing"


@pytest.mark.asyncio
async def test_post_cancellation_joins_create_before_delete(monkeypatch, tmp_path):
    post_started = asyncio.Event()
    permit_post = asyncio.Event()
    order = []
    monkeypatch.setattr(kubernetes, "_namespace", lambda: "testing")
    monkeypatch.setattr(kubernetes, "_template", _template)

    async def to_thread(function, method, path, body=None):
        if method == "POST":
            post_started.set()
            await permit_post.wait()
            order.append("post")
        return {}

    async def delete(namespace, name):
        order.append("delete")

    monkeypatch.setattr(kubernetes.asyncio, "to_thread", to_thread)
    monkeypatch.setattr(kubernetes, "_delete", delete)
    task = asyncio.create_task(
        kubernetes.run_in_kubernetes_job("upload", {}, tmp_path / "events", 10)
    )
    await post_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert order == []
    permit_post.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert order == ["post", "delete"]


@pytest.mark.asyncio
async def test_cleanup_error_still_releases_worker_slot(monkeypatch, tmp_path):
    monkeypatch.setattr(kubernetes, "_namespace", lambda: "testing")
    monkeypatch.setattr(kubernetes, "_template", _template)
    monkeypatch.setattr(kubernetes, "_request", lambda *args, **kwargs: {})

    async def failed_delete(namespace, name):
        raise kubernetes.KubernetesJobCleanupError("not deleted")

    monkeypatch.setattr(kubernetes, "_delete", failed_delete)
    with pytest.raises(kubernetes.KubernetesJobCleanupError):
        await kubernetes.run_in_kubernetes_job("upload", {}, tmp_path / "events", 0.01)
    assert kubernetes._WORKER_SLOTS.acquire(blocking=False)
    kubernetes._WORKER_SLOTS.release()
