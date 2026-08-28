"""Regression test for one pinned LG-to-PG translator pipeline."""

import json
from pathlib import Path

import pytest

from .golden_utils import (
    PipelineError,
    find_dlg_executable,
    first_json_difference,
    format_json_value,
    load_json,
    run_pipeline,
    sha256_file,
)


GOLDEN_DIR = Path(__file__).resolve().parent
MANIFEST_PATH = GOLDEN_DIR / "manifest.json"


def _load_manifest():
    with MANIFEST_PATH.open(encoding="utf-8") as stream:
        return json.load(stream)


def _assert_digest(path: Path, expected_digest: str, description: str) -> None:
    actual_digest = sha256_file(path)
    assert actual_digest == expected_digest, (
        f"{description} SHA-256 mismatch\n"
        f"Path: {path}\n"
        f"Expected: {expected_digest}\n"
        f"Actual:   {actual_digest}"
    )


def test_single_graph_matches_legacy_outputs(tmp_path):
    """Compare current PGT, partitioned PGT and PG with pinned legacy JSON."""
    manifest = _load_manifest()
    case = manifest["cases"][0]
    input_path = GOLDEN_DIR / case["input"]
    expected_dir = GOLDEN_DIR / case["expected_dir"]

    _assert_digest(
        input_path,
        manifest["graph_source"]["sha256"],
        "Pinned logical graph",
    )
    for stage, fixture in case["expected"].items():
        _assert_digest(
            expected_dir / fixture["file"],
            fixture["sha256"],
            f"Legacy {stage} fixture",
        )

    try:
        actual_outputs = run_pipeline(
            find_dlg_executable(), input_path, tmp_path, case["pipeline"]
        )
    except PipelineError as error:
        pytest.fail(str(error), pytrace=False)

    for stage in ("PGT", "PGT-P", "PG"):
        fixture = case["expected"][stage]
        expected_path = expected_dir / fixture["file"]
        actual_path = actual_outputs[stage]
        difference = first_json_difference(
            load_json(expected_path), load_json(actual_path)
        )
        if difference:
            pytest.fail(
                f"Stage: {stage}\n"
                f"Path: {difference.path}\n"
                f"Reason: {difference.reason}\n"
                f"Expected: {format_json_value(difference.expected)}\n"
                f"Actual:   {format_json_value(difference.actual)}",
                pytrace=False,
            )


def test_comparator_ignores_object_key_order():
    """JSON formatting and object key order are not wire-format differences."""
    expected = {"outer": {"first": 1, "second": 2}}
    actual = {"outer": {"second": 2, "first": 1}}
    assert first_json_difference(expected, actual) is None


def test_comparator_reports_first_nested_difference():
    """A field mutation is reported with the exact JSON path."""
    expected = [{"node": "#0", "weight": 1}]
    actual = [{"node": "#1", "weight": 1}]
    difference = first_json_difference(expected, actual)
    assert difference is not None
    assert difference.path == "$[0].node"
    assert difference.expected == "#0"
    assert difference.actual == "#1"


def test_comparator_detects_mutated_golden_fixture():
    """A mutation of a real fixture is caught without altering the fixture file."""
    pgt_path = GOLDEN_DIR / "expected/ArrayLoop/metis/pgt.json"
    expected = load_json(pgt_path)
    actual = json.loads(json.dumps(expected))
    actual[0]["name"] = "mutated-array"

    difference = first_json_difference(expected, actual)
    assert difference is not None
    assert difference.path == "$[0].name"
    assert difference.expected == "array"
    assert difference.actual == "mutated-array"
