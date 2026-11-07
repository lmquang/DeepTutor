from __future__ import annotations

import asyncio
import time
import threading

import pytest
from pathlib import Path
from types import SimpleNamespace

from deeptutor.api.routers import knowledge
from deeptutor.multi_user.context import (
    get_current_user_or_none,
    reset_current_user,
    set_current_user,
)
from deeptutor.multi_user.models import CurrentUser, UserScope


@pytest.mark.parametrize(
    "handler",
    [
        "get_knowledge_base_details",
        "list_kb_raw_files",
        "get_reindex_config",
        "get_indexing_run",
        "get_indexing_readiness",
        "get_progress",
        "get_linked_folders",
        "get_github_sources",
        "get_web_sources",
    ],
)
def test_kb_read_handlers_offload_entire_operation(monkeypatch, handler):
    helper_name = "_" + handler + "_sync"
    main_thread = threading.get_ident()
    calls = []

    def read(*args):
        assert threading.get_ident() != main_thread
        calls.append(args)
        return {"ok": True}

    monkeypatch.setattr(knowledge, helper_name, read)
    assert asyncio.run(getattr(knowledge, handler)("kb")) == {"ok": True}
    assert calls[0][0] == "kb"


def test_linked_folders_read_offloads_resolution_and_preserves_owner(monkeypatch, tmp_path: Path):
    owner = CurrentUser(
        id="responsive-owner",
        username="responsive-owner",
        role="user",
        scope=UserScope(kind="user", user_id="responsive-owner", root=tmp_path),
    )
    resource = SimpleNamespace(name="kb")

    def slow_resolve(name: str):
        assert name == "kb"
        assert get_current_user_or_none() == owner
        time.sleep(0.15)
        return resource

    manager = SimpleNamespace(get_linked_folders=lambda _name: [])
    monkeypatch.setattr(knowledge, "resolve_kb", slow_resolve)
    monkeypatch.setattr(knowledge, "manager_for_resource", lambda _resource: manager)

    async def run():
        read = asyncio.create_task(knowledge.get_linked_folders("kb"))
        await asyncio.sleep(0.02)
        assert not read.done()
        return await read

    token = set_current_user(owner)
    try:
        assert asyncio.run(run()) == []
    finally:
        reset_current_user(token)
