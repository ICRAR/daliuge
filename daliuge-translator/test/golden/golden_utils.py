"""Helpers for deterministic, stage-by-stage DALiuGE golden tests."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping


STAGE_FILENAMES = {
    "LG": "lg.json",
    "PGT": "pgt.json",
    "PGT-P": "pgtp.json",
    "PG": "pg.json",
}


class PipelineError(RuntimeError):
    """Raised when a translator CLI stage cannot produce its output."""


@dataclass(frozen=True)
class JsonDifference:
    """The first structural difference between two JSON-compatible values."""

    path: str
    expected: Any
    actual: Any
    reason: str


def sha256_file(path: Path) -> str:
    """Return the lowercase SHA-256 digest for *path*."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def find_dlg_executable() -> str:
    """Locate the ``dlg`` console script belonging to the active environment."""
    configured = os.environ.get("DLG_CLI")
    if configured:
        return configured

    executable = shutil.which("dlg")
    if executable:
        return executable

    sibling = Path(sys.executable).with_name("dlg")
    if sibling.is_file():
        return str(sibling)

    raise PipelineError(
        "Cannot locate the dlg CLI. Activate the DALiuGE virtual environment "
        "or set DLG_CLI to the console-script path."
    )


def _run_stage(stage: str, command: Iterable[str], output_path: Path) -> None:
    command = list(command)
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        rendered = " ".join(command)
        raise PipelineError(
            f"{stage} failed with exit code {result.returncode}\n"
            f"Command: {rendered}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    if not output_path.is_file():
        raise PipelineError(f"{stage} did not create {output_path}")

    try:
        with output_path.open(encoding="utf-8") as stream:
            json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        raise PipelineError(f"{stage} produced invalid JSON at {output_path}") from error


def run_pipeline(
    dlg_executable: str,
    input_graph: Path,
    output_dir: Path,
    pipeline: Mapping[str, Any],
) -> Dict[str, Path]:
    """Run fill, unroll, partition and map with manifest-defined options."""
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        stage: output_dir / filename for stage, filename in STAGE_FILENAMES.items()
    }

    fill_config = pipeline["fill"]
    fill_command = [
        dlg_executable,
        "fill",
        "-L",
        str(input_graph),
        "-R",
        str(pipeline["reproducibility"]),
    ]
    for parameter in fill_config["parameters"]:
        fill_command.extend(["-p", parameter])
    fill_command.extend(["-o", str(outputs["LG"]), "-f"])
    _run_stage("LG/fill", fill_command, outputs["LG"])

    unroll_config = pipeline["unroll"]
    unroll_command = [
        dlg_executable,
        "unroll",
        "-L",
        str(outputs["LG"]),
        "-p",
        pipeline["oid_prefix"],
        "--app",
        str(unroll_config["app"]),
        "-o",
        str(outputs["PGT"]),
        "-f",
    ]
    if unroll_config["zerorun"]:
        unroll_command.append("-z")
    _run_stage("PGT/unroll", unroll_command, outputs["PGT"])

    partition_config = pipeline["partition"]
    partition_command = [
        dlg_executable,
        "partition",
        "-P",
        str(outputs["PGT"]),
        "-a",
        partition_config["algorithm"],
        "-N",
        str(partition_config["partitions"]),
        "-i",
        str(partition_config["islands"]),
        "-o",
        str(outputs["PGT-P"]),
        "-f",
    ]
    _run_stage("PGT-P/partition", partition_command, outputs["PGT-P"])

    map_config = pipeline["map"]
    map_command = [
        dlg_executable,
        "map",
        "-P",
        str(outputs["PGT-P"]),
        "-N",
        ",".join(map_config["nodes"]),
        "-i",
        str(map_config["islands"]),
        "-o",
        str(outputs["PG"]),
        "-f",
    ]
    _run_stage("PG/map", map_command, outputs["PG"])
    return outputs


def _child_path(path: str, key: str) -> str:
    if key.isidentifier():
        return f"{path}.{key}"
    return f"{path}[{json.dumps(key, ensure_ascii=False)}]"


def first_json_difference(
    expected: Any, actual: Any, path: str = "$"
) -> JsonDifference | None:
    """Return the first exact structural difference, ignoring object key order."""
    if type(expected) is not type(actual):
        return JsonDifference(
            path,
            expected,
            actual,
            f"type differs ({type(expected).__name__} != {type(actual).__name__})",
        )

    if isinstance(expected, dict):
        missing = sorted(set(expected) - set(actual))
        if missing:
            key = missing[0]
            return JsonDifference(
                _child_path(path, key), expected[key], "<missing>", "key is missing"
            )

        unexpected = sorted(set(actual) - set(expected))
        if unexpected:
            key = unexpected[0]
            return JsonDifference(
                _child_path(path, key), "<missing>", actual[key], "unexpected key"
            )

        for key in sorted(expected):
            difference = first_json_difference(
                expected[key], actual[key], _child_path(path, key)
            )
            if difference:
                return difference
        return None

    if isinstance(expected, list):
        if len(expected) != len(actual):
            return JsonDifference(
                path,
                len(expected),
                len(actual),
                "array length differs",
            )
        for index, expected_item in enumerate(expected):
            difference = first_json_difference(
                expected_item, actual[index], f"{path}[{index}]"
            )
            if difference:
                return difference
        return None

    if expected != actual:
        return JsonDifference(path, expected, actual, "value differs")
    return None


def load_json(path: Path) -> Any:
    """Load a UTF-8 JSON document."""
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def format_json_value(value: Any, limit: int = 500) -> str:
    """Render a bounded value for an actionable pytest failure message."""
    rendered = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if len(rendered) > limit:
        return rendered[:limit] + "..."
    return rendered
