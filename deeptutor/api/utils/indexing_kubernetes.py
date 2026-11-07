"""Run trusted indexing work in a short-lived Kubernetes Job.

This module deliberately uses the in-cluster HTTP API directly.  Keeping the
client here small avoids adding the Kubernetes Python client to every API pod.
The Job template is administrator-owned; only its name and worker arguments
are changed at runtime.
"""

from __future__ import annotations

import asyncio
import copy
import json
import math
import os
from pathlib import Path
import pickle
import secrets
import ssl
import threading
import time
from typing import Any
import urllib.error
import urllib.request

from deeptutor.runtime.isolated_worker import (
    IsolatedWorkerTimeout,
    _read_envelope,
)

_CHILD_ENTRY = "deeptutor.api.utils.indexing_worker:_run_indexing_child"
_OPERATIONS = frozenset({"initialization", "upload", "reindex"})
_SA_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")
_DEFAULT_TEMPLATE = Path("/etc/deeptutor/indexing-job.json")


class KubernetesJobError(RuntimeError):
    """The Kubernetes API or indexing Job reported a failure."""


class KubernetesJobCleanupError(KubernetesJobError):
    """The Job could not be confirmed deleted; its IPC directory must be retained."""


def _worker_limit() -> int:
    try:
        return max(1, int(os.environ.get("DEEPTUTOR_ISOLATED_WORKERS", "2") or "2"))
    except ValueError:
        return 2


_WORKER_SLOTS = threading.BoundedSemaphore(_worker_limit())


def _namespace() -> str:
    value = os.environ.get("DEEPTUTOR_INDEXING_NAMESPACE")
    return value or (_SA_DIR / "namespace").read_text(encoding="utf-8").strip()


def _template() -> dict[str, Any]:
    path = Path(os.environ.get("DEEPTUTOR_INDEXING_JOB_TEMPLATE_PATH", str(_DEFAULT_TEMPLATE)))
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise KubernetesJobError("Indexing Job template must contain a JSON object")
    return value


def _connection() -> tuple[str, str, ssl.SSLContext]:
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    token = (_SA_DIR / "token").read_text(encoding="utf-8").strip()
    context = ssl.create_default_context(cafile=str(_SA_DIR / "ca.crt"))
    return f"https://{host}:{port}", token, context


def _request(method: str, path: str, body: object | None = None) -> dict[str, Any]:
    base, token, context = _connection()
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, context=context, timeout=30) as response:  # nosec B310 - fixed in-cluster endpoint
            raw = response.read()
    except urllib.error.HTTPError as exc:
        # Never include headers (and therefore the service-account token).
        detail = exc.read(2048).decode("utf-8", errors="replace")
        raise KubernetesJobError(
            f"Kubernetes API {method} {path} failed ({exc.code}): {detail}"
        ) from exc
    except urllib.error.URLError as exc:
        raise KubernetesJobError(f"Kubernetes API {method} {path} failed: {exc.reason}") from exc
    if not raw:
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise KubernetesJobError("Kubernetes API returned an invalid response")
    return value


def _job(
    template: dict[str, Any], name: str, namespace: str, request: Path, result: Path, timeout: float
) -> dict[str, Any]:
    job = copy.deepcopy(template)
    job.setdefault("apiVersion", "batch/v1")
    job.setdefault("kind", "Job")
    metadata = job.setdefault("metadata", {})
    metadata["name"] = name
    metadata["namespace"] = namespace
    spec = job.setdefault("spec", {})
    spec["backoffLimit"] = 0
    spec["activeDeadlineSeconds"] = max(1, math.ceil(timeout))
    spec.setdefault("ttlSecondsAfterFinished", 300)
    try:
        containers = spec["template"]["spec"]["containers"]
        container = containers[0]
    except (KeyError, IndexError, TypeError) as exc:
        raise KubernetesJobError("Indexing Job template has no worker container") from exc
    container["args"] = [str(request), str(result)]
    return job


async def _delete(namespace: str, name: str) -> None:
    path = f"/apis/batch/v1/namespaces/{namespace}/jobs/{name}?propagationPolicy=Foreground"
    try:
        await asyncio.to_thread(
            _request,
            "DELETE",
            path,
            {"kind": "DeleteOptions", "apiVersion": "v1", "propagationPolicy": "Foreground"},
        )
    except KubernetesJobError as exc:
        if "(404)" not in str(exc):
            raise
    # Foreground deletion may return before the object disappears.
    for _ in range(150):
        try:
            await asyncio.to_thread(_request, "GET", path.split("?")[0])
        except KubernetesJobError as exc:
            if "(404)" in str(exc):
                return
            raise
        await asyncio.sleep(0.1)
    raise KubernetesJobCleanupError(
        f"Timed out waiting for Kubernetes Job {name} deletion; retain its IPC directory"
    )


async def _finish_delete(namespace: str, name: str) -> None:
    """Wait for deletion even when the calling task is being cancelled."""
    cleanup = asyncio.create_task(_delete(namespace, name))
    try:
        await asyncio.shield(cleanup)
    except asyncio.CancelledError:
        # Shield prevents cancellation of cleanup but does not itself wait after
        # the caller receives cancellation, so explicitly join it here.
        try:
            await cleanup
        except Exception as exc:
            raise KubernetesJobCleanupError(
                f"Could not confirm Kubernetes Job {name} deletion; retain its IPC directory"
            ) from exc
        raise
    except Exception as exc:
        if isinstance(exc, KubernetesJobCleanupError):
            raise
        raise KubernetesJobCleanupError(
            f"Could not confirm Kubernetes Job {name} deletion; retain its IPC directory"
        ) from exc


def _write_request_secure(path: Path, kind: str, payload: dict[str, Any], event_path: str) -> None:
    request = {
        "callable_path": _CHILD_ENTRY,
        "args": (kind, payload, event_path),
        "kwargs": {},
    }
    data = pickle.dumps(request, protocol=pickle.HIGHEST_PROTOCOL)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(data)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


async def run_in_kubernetes_job(
    kind: str, payload: dict[str, Any], event_path: str | os.PathLike[str], timeout: float
) -> Any:
    """Execute one known indexing operation using an administrator Job template.

    ``event_path`` and its parent must be on storage shared by API and worker
    pods.  The caller owns that directory and must not remove it until this
    coroutine returns (including cancellation cleanup).
    """
    if kind not in _OPERATIONS:
        raise ValueError(f"Unknown indexing worker operation: {kind}")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be a finite positive number")
    events = Path(event_path).resolve()
    directory = events.parent
    if not directory.is_dir():
        raise ValueError("event_path parent must already exist")
    request_path = directory / "request.pickle"
    result_path = directory / "result.pickle"
    _write_request_secure(request_path, kind, payload, str(events))

    namespace = _namespace()
    name = f"deeptutor-index-{secrets.token_hex(8)}"
    collection = f"/apis/batch/v1/namespaces/{namespace}/jobs"
    resource = f"{collection}/{name}"
    creation_attempted = False
    acquired = False
    try:
        while not acquired:
            acquired = _WORKER_SLOTS.acquire(blocking=False)
            if not acquired:
                await asyncio.sleep(0.02)
        body = _job(_template(), name, namespace, request_path, result_path, timeout)
        creation_attempted = True
        create = asyncio.create_task(asyncio.to_thread(_request, "POST", collection, body))
        try:
            await asyncio.shield(create)
        except asyncio.CancelledError:
            # A thread cannot be cancelled.  Join the POST before deleting so
            # it cannot create the Job after cleanup has already completed.
            try:
                await create
            except Exception:
                pass
            raise
        deadline = time.monotonic() + timeout
        while True:
            if time.monotonic() >= deadline:
                raise IsolatedWorkerTimeout(f"Kubernetes indexing Job exceeded {timeout:g} seconds")
            status = (await asyncio.to_thread(_request, "GET", resource)).get("status") or {}
            if status.get("succeeded"):
                return await asyncio.to_thread(_read_envelope, result_path, _CHILD_ENTRY)
            if status.get("failed"):
                conditions = status.get("conditions") or []
                reason = next(
                    (
                        c.get("message") or c.get("reason")
                        for c in conditions
                        if c.get("status") == "True"
                    ),
                    None,
                )
                raise KubernetesJobError(
                    f"Kubernetes indexing Job failed{': ' + str(reason) if reason else ''}"
                )
            await asyncio.sleep(0.2)
    finally:
        try:
            if creation_attempted:
                await _finish_delete(namespace, name)
        finally:
            if acquired:
                _WORKER_SLOTS.release()


__all__ = [
    "KubernetesJobCleanupError",
    "KubernetesJobError",
    "run_in_kubernetes_job",
]
