"""Incremental source receipts must follow successful version publication."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("publish_fails", [False, True])
async def test_incremental_receipts_only_include_published_loaded_sources(
    monkeypatch, tmp_path, existing, publish_fails
):
    from deeptutor.services.rag.pipelines.llamaindex import pipeline as module

    monkeypatch.setattr(module.LlamaIndexPipeline, "_configure_settings", lambda self: None)
    monkeypatch.setattr(module.LlamaIndexPipeline, "_verify_embedding_connectivity", AsyncMock())
    monkeypatch.setattr(module, "set_progress_callback", lambda callback: None)
    documents = [
        SimpleNamespace(metadata={"file_path": "accepted.py"}),
        SimpleNamespace(metadata={"file_path": "accepted.py"}),
        SimpleNamespace(metadata={}),
    ]
    monkeypatch.setattr(
        module,
        "LlamaIndexDocumentLoader",
        lambda logger: SimpleNamespace(load=AsyncMock(return_value=documents)),
    )
    plan = SimpleNamespace(
        existing_storage=tmp_path / "old" if existing else None,
        storage_dir=tmp_path / "new",
    )
    monkeypatch.setattr(module.storage, "resolve_add_storage_plan", lambda *args: plan)
    monkeypatch.setattr(module.storage, "insert_documents", lambda *args: len(documents))
    monkeypatch.setattr(module.storage, "create_index", lambda *args, **kwargs: len(documents))
    monkeypatch.setattr(module, "VisualAssetStore", lambda path: SimpleNamespace(publish=Mock()))
    pipeline = module.LlamaIndexPipeline(kb_base_dir=str(tmp_path), signature_provider=lambda: None)
    receipts = []

    async def publish(*args):
        assert receipts == []
        if publish_fails:
            raise RuntimeError("publication failed")

    monkeypatch.setattr(pipeline, "_publish_verified", publish)
    monkeypatch.setattr(pipeline, "_cleanup_failed_version_dir", lambda *args: None)
    operation = pipeline.add_documents(
        "kb", ["accepted.py", "skipped.py"], indexed_file_callback=receipts.append
    )
    if publish_fails:
        with pytest.raises(RuntimeError, match="publication failed"):
            await operation
        assert receipts == []
    else:
        assert await operation is True
        assert receipts == [["accepted.py"]]
