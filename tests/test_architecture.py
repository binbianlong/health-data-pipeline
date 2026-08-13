from __future__ import annotations

import ast
from pathlib import Path

PACKAGE = Path(__file__).parents[1] / "src" / "health_data_pipeline"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)
    return imported


def _assert_packages_do_not_import(
    packages: tuple[str, ...],
    forbidden: tuple[str, ...],
) -> None:
    violations: list[str] = []
    for package_name in packages:
        for path in (PACKAGE / package_name).rglob("*.py"):
            violations.extend(
                f"{path.relative_to(PACKAGE)} imports {imported}"
                for imported in _imports(path)
                if any(
                    imported == prefix or imported.startswith(f"{prefix}.") for prefix in forbidden
                )
            )

    assert violations == []


def test_health_semantics_are_independent_of_workflows_and_adapters() -> None:
    _assert_packages_do_not_import(
        ("health",),
        (
            "health_data_pipeline.integrations",
            "health_data_pipeline.ops",
            "health_data_pipeline.runtime",
            "health_data_pipeline.setup",
            "health_data_pipeline.state",
            "health_data_pipeline.sync",
            "health_data_pipeline.views",
        ),
    )


def test_state_does_not_depend_on_runtime_or_external_adapters() -> None:
    _assert_packages_do_not_import(
        ("state",),
        (
            "health_data_pipeline.integrations",
            "health_data_pipeline.ops",
            "health_data_pipeline.runtime",
            "health_data_pipeline.setup",
            "health_data_pipeline.sync",
            "health_data_pipeline.views",
        ),
    )


def test_inner_features_do_not_depend_on_runtime_or_external_adapters() -> None:
    _assert_packages_do_not_import(
        ("ops", "setup", "sync", "views"),
        (
            "health_data_pipeline.integrations",
            "health_data_pipeline.runtime",
        ),
    )


def test_runtime_is_not_imported_outside_runtime() -> None:
    _assert_packages_do_not_import(
        ("health", "integrations", "ops", "setup", "state", "sync", "views"),
        ("health_data_pipeline.runtime",),
    )


def test_package_root_contains_no_feature_facades() -> None:
    assert {path.name for path in PACKAGE.glob("*.py")} == {"__init__.py"}
