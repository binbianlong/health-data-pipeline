"""The single mutable GCS HEAD and its compare-and-swap contract."""

from __future__ import annotations

import json
from dataclasses import dataclass

from health_data_pipeline.state.canonical import (
    CANONICALIZATION_VERSION,
    canonical_json_bytes,
    sha256_hex,
)
from health_data_pipeline.state.models import DataHead
from health_data_pipeline.state.objects import (
    ObjectNotFound,
    ObjectStore,
    StorageIntegrityError,
)

HEAD_OBJECT_NAME = "state/data-head.json"


@dataclass(frozen=True, slots=True)
class HeadVersion:
    """A parsed HEAD and the GCS generation needed for its next CAS."""

    head: DataHead | None
    object_generation: int | None

    def __post_init__(self) -> None:
        if (self.head is None) != (self.object_generation is None):
            msg = "head and object_generation must either both exist or both be absent"
            raise ValueError(msg)


class HeadRepository:
    """Read and compare-and-swap the single mutable data HEAD."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        object_name: str = HEAD_OBJECT_NAME,
    ) -> None:
        self._store = store
        self._object_name = object_name

    def read(self) -> HeadVersion:
        try:
            stored = self._store.get(self._object_name)
        except ObjectNotFound:
            return HeadVersion(head=None, object_generation=None)
        actual_hash = sha256_hex(stored.data)
        metadata_hash = stored.metadata.get("canonical-sha256")
        if metadata_hash is not None and metadata_hash != actual_hash:
            msg = f"HEAD integrity metadata mismatch for {self._object_name}"
            raise StorageIntegrityError(msg)
        try:
            payload = json.loads(stored.data)
            head = DataHead.model_validate(payload)
        except (json.JSONDecodeError, ValueError) as exc:
            msg = f"invalid HEAD object: {self._object_name}"
            raise StorageIntegrityError(msg) from exc
        if canonical_json_bytes(head) != stored.data:
            msg = f"HEAD is not canonical FHCJ-1: {self._object_name}"
            raise StorageIntegrityError(msg)
        return HeadVersion(head=head, object_generation=stored.generation)

    def compare_and_swap(
        self,
        candidate: DataHead,
        *,
        base: HeadVersion,
    ) -> HeadVersion:
        """Commit a next HEAD exactly once, with no retry or deletion."""

        _validate_next_head(candidate, base)
        canonical_content = canonical_json_bytes(candidate)
        canonical_hash = sha256_hex(canonical_content)
        expected_generation = base.object_generation or 0
        stored = self._store.put(
            self._object_name,
            canonical_content,
            if_generation_match=expected_generation,
            metadata={
                "canonicalization": CANONICALIZATION_VERSION,
                "canonical-sha256": canonical_hash,
                "logical-content-type": "application/json",
            },
            content_type="application/json",
            cache_control="no-store",
        )
        return HeadVersion(head=candidate, object_generation=stored.generation)


def _validate_next_head(candidate: DataHead, base: HeadVersion) -> None:
    if base.head is None:
        if candidate.generation != 1 or candidate.parent_generation != 0:
            msg = "the first HEAD must be generation 1 with parent_generation 0"
            raise ValueError(msg)
        return
    if candidate.generation != base.head.generation + 1:
        msg = "candidate HEAD generation must increment the base by one"
        raise ValueError(msg)
    if candidate.parent_generation != base.head.generation:
        msg = "candidate HEAD parent_generation must equal the base generation"
        raise ValueError(msg)
