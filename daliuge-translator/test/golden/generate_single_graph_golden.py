"""Manually generate candidate fixtures from a verified legacy checkout."""

import argparse
import json
import subprocess
from pathlib import Path

from .golden_utils import run_pipeline, sha256_file


GOLDEN_DIR = Path(__file__).resolve().parent
MANIFEST_PATH = GOLDEN_DIR / "manifest.json"


def _parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Generate single-graph fixtures into a review directory. "
            "This command never overwrites committed expected files."
        )
    )
    parser.add_argument(
        "--dlg",
        required=True,
        help="Path to dlg installed in the isolated legacy virtual environment",
    )
    parser.add_argument(
        "--legacy-repo",
        required=True,
        type=Path,
        help="Legacy DALiuGE worktree whose HEAD must match the manifest",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Empty/review directory for generated outputs",
    )
    return parser.parse_args()


def _git_head(repository: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip()


def main():
    args = _parse_args()
    with MANIFEST_PATH.open(encoding="utf-8") as stream:
        manifest = json.load(stream)

    expected_commit = manifest["legacy_daliuge"]["commit"]
    actual_commit = _git_head(args.legacy_repo)
    if actual_commit != expected_commit:
        raise SystemExit(
            "Legacy checkout mismatch:\n"
            f"Expected: {expected_commit}\n"
            f"Actual:   {actual_commit}"
        )

    committed_expected = (GOLDEN_DIR / manifest["cases"][0]["expected_dir"]).resolve()
    output_dir = args.output_dir.resolve()
    if output_dir == committed_expected or committed_expected in output_dir.parents:
        raise SystemExit("Refusing to write into the committed expected fixture directory")

    case = manifest["cases"][0]
    input_path = GOLDEN_DIR / case["input"]
    expected_input_digest = manifest["graph_source"]["sha256"]
    actual_input_digest = sha256_file(input_path)
    if actual_input_digest != expected_input_digest:
        raise SystemExit(
            "Pinned logical graph mismatch:\n"
            f"Expected: {expected_input_digest}\n"
            f"Actual:   {actual_input_digest}"
        )
    outputs = run_pipeline(args.dlg, input_path, output_dir, case["pipeline"])

    for stage in ("LG", "PGT", "PGT-P", "PG"):
        output = outputs[stage]
        print(f"{stage}: {output} sha256={sha256_file(output)}")
    print("Review these files before copying PGT, PGT-P and PG into expected/.")


if __name__ == "__main__":
    main()
