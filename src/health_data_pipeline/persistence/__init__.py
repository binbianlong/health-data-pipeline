"""Shared persistence primitives."""

from health_data_pipeline.persistence.versioned_state import (
    VersionedObject,
    read_state,
    utc_timestamp,
    write_state,
)

__all__ = ["VersionedObject", "read_state", "utc_timestamp", "write_state"]
