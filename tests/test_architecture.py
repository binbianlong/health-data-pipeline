from __future__ import annotations

import ast
from pathlib import Path

PACKAGE = Path(__file__).parents[1] / "src" / "health_data_pipeline"
NEW_PACKAGES = (
    PACKAGE / "application",
    PACKAGE / "presentation",
    PACKAGE / "adapters",
    PACKAGE / "persistence",
)
LEGACY_ENTRYPOINTS = {
    "health_data_pipeline.job",
    "health_data_pipeline.notion_adapter",
    "health_data_pipeline.pipeline",
    "health_data_pipeline.runtime",
    "health_data_pipeline.views",
    "health_data_pipeline.web",
}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)
    return imported


def test_new_modules_do_not_depend_on_legacy_entrypoints() -> None:
    violations: list[str] = []
    for package in NEW_PACKAGES:
        for path in package.rglob("*.py"):
            violations.extend(
                f"{path.relative_to(PACKAGE)} imports {imported}"
                for imported in _imports(path)
                if any(
                    imported == legacy or imported.startswith(f"{legacy}.")
                    for legacy in LEGACY_ENTRYPOINTS
                )
            )

    assert violations == []


def test_shared_metric_catalog_does_not_depend_on_adapters() -> None:
    for path in (PACKAGE / "data_types.py", PACKAGE / "metrics.py"):
        assert not any(
            imported == "health_data_pipeline.adapters"
            or imported.startswith("health_data_pipeline.adapters.")
            for imported in _imports(path)
        )
