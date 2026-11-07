from __future__ import annotations

import asyncio
import json
from pathlib import Path

from deeptutor.knowledge import add_documents as module
from deeptutor.knowledge.add_documents import DocumentAdder


def _adder(tmp_path: Path, provider: str = "llamaindex") -> DocumentAdder:
    kb = tmp_path / "kb"
    (kb / "raw").mkdir(parents=True)
    version = kb / "version-1"
    version.mkdir()
    if provider == "graphrag":
        (version / "output").mkdir()
        (version / "output" / "entities.parquet").write_bytes(b"placeholder")
    (version / "docstore.json").write_text("{}")
    (version / "index_store.json").write_text("{}")
    (version / "meta.json").write_text(
        json.dumps({"provider": provider, "signature": provider, "version": "version-1"})
    )
    return DocumentAdder("kb", str(tmp_path), rag_provider=provider)


def _files(adder: DocumentAdder, count: int, size: int = 1) -> list[Path]:
    result = []
    for number in range(count):
        path = adder.raw_dir / f"{number}.txt"
        path.write_bytes(b"x" * size)
        result.append(path)
    return result


def test_llamaindex_batches_by_count_and_hashes_confirmed(monkeypatch, tmp_path: Path) -> None:
    adder = _adder(tmp_path)
    files = _files(adder, 5)
    monkeypatch.setattr(module, "LLAMAINDEX_BATCH_MAX_FILES", 2)
    calls = []

    class Service:
        def __init__(self, **_kwargs):
            pass

        async def add_documents(self, _kb, paths, **kwargs):
            calls.append(paths)
            kwargs["indexed_file_callback"](paths)
            return True

    monkeypatch.setattr(module, "RAGService", Service)
    result = asyncio.run(adder.process_new_documents(files))
    assert [len(call) for call in calls] == [2, 2, 1]
    assert result.processed_files == files
    assert result.failures == []
    assert set(adder.get_ingested_hashes()) == {path.name for path in files}


def test_llamaindex_batches_by_bytes_with_large_file_singleton(monkeypatch, tmp_path: Path) -> None:
    adder = _adder(tmp_path)
    files = _files(adder, 3, 3)
    monkeypatch.setattr(module, "LLAMAINDEX_BATCH_MAX_BYTES", 4)
    assert adder._llamaindex_batches(files) == [[files[0]], [files[1]], [files[2]]]
    large = adder.raw_dir / "large.txt"
    large.write_bytes(b"12345")
    assert adder._llamaindex_batches([files[0], large, files[1]]) == [
        [files[0]],
        [large],
        [files[1]],
    ]


def test_partial_receipt_on_failure_attributes_only_confirmed(monkeypatch, tmp_path: Path) -> None:
    adder = _adder(tmp_path)
    files = _files(adder, 3)

    class Service:
        def __init__(self, **_kwargs):
            pass

        async def add_documents(self, _kb, paths, **kwargs):
            kwargs["indexed_file_callback"]([paths[0]])
            raise RuntimeError("publication failed")

    monkeypatch.setattr(module, "RAGService", Service)
    result = asyncio.run(adder.process_new_documents(files))
    assert result.processed_files == [files[0]]
    assert [failure.file_path for failure in result.failures] == files[1:]
    assert set(adder.get_ingested_hashes()) == {files[0].name}


def test_success_without_receipt_does_not_claim_skipped_files(monkeypatch, tmp_path: Path) -> None:
    adder = _adder(tmp_path)
    files = _files(adder, 2)

    class Service:
        def __init__(self, **_kwargs):
            pass

        async def add_documents(self, *_args, **_kwargs):
            return True

    monkeypatch.setattr(module, "RAGService", Service)
    result = asyncio.run(adder.process_new_documents(files))
    assert result.processed_files == []
    assert [failure.file_path for failure in result.failures] == files
    assert adder.get_ingested_hashes() == {}


def test_later_batch_failure_preserves_prior_success(monkeypatch, tmp_path: Path) -> None:
    adder = _adder(tmp_path)
    files = _files(adder, 4)
    monkeypatch.setattr(module, "LLAMAINDEX_BATCH_MAX_FILES", 2)
    calls = []

    class Service:
        def __init__(self, **_kwargs):
            pass

        async def add_documents(self, _kb, paths, **kwargs):
            calls.append(paths)
            if len(calls) == 2:
                raise RuntimeError("embedding unavailable")
            kwargs["indexed_file_callback"](paths)
            return True

    monkeypatch.setattr(module, "RAGService", Service)
    result = asyncio.run(adder.process_new_documents(files))
    assert len(calls) == 2  # No singleton fallback for the failed batch.
    assert result.processed_files == files[:2]
    assert [failure.file_path for failure in result.failures] == files[2:]
    assert set(adder.get_ingested_hashes()) == {path.name for path in files[:2]}


def test_receipts_match_canonical_paths_and_exclude_unknown_sources(monkeypatch, tmp_path):
    adder = _adder(tmp_path)
    files = _files(adder, 2)
    monkeypatch.chdir(tmp_path)

    class Service:
        def __init__(self, **_kwargs):
            pass

        async def add_documents(self, _kb, paths, **kwargs):
            kwargs["indexed_file_callback"](["kb/raw/0.txt", "unrequested.txt"])
            return True

    monkeypatch.setattr(module, "RAGService", Service)
    result = asyncio.run(adder.process_new_documents(files))
    assert result.processed_files == files[:1]
    assert [failure.file_path for failure in result.failures] == files[1:]
    assert set(adder.get_ingested_hashes()) == {files[0].name}


def test_non_llamaindex_provider_keeps_single_file_calls(monkeypatch, tmp_path: Path) -> None:
    adder = _adder(tmp_path, provider="graphrag")
    files = _files(adder, 2)
    calls = []

    class Service:
        def __init__(self, **_kwargs):
            pass

        async def add_documents(self, _kb, paths):
            calls.append(paths)
            return True

    monkeypatch.setattr(module, "RAGService", Service)
    result = asyncio.run(adder.process_new_documents(files))
    assert calls == [[str(files[0])], [str(files[1])]]
    assert result.processed_files == files
