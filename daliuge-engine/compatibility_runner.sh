#!/usr/bin/env bash
set -uo pipefail

# Directory containing files to process
DIR="${1:-/home/00087932/github/test_runner_graphs/}"

# Track overall failure
overall_status=0

if [ ! -d "$DIR" ]; then
    echo "Directory $DIR not found!"
    exit 1
fi

# Find all files
mapfile -t FILES < <(find $DIR -iname "*.graph" | sort)

if [ ${#FILES[@]} -eq 0 ]; then
    echo "No files found in $DIR"
    exit 0
fi

echo "Found ${#FILES[@]} files to process"

# Process files sequentially
for file in "${FILES[@]}"; do

    echo "→ Processing: $file"

    # Run the Python script and redirect stdout/stderr to a per-file log
    if python graph_compatibility.py "$file" 2>&1; then
        echo "✓ PASS: $file"
    else
        echo "✗ FAIL: $file" >&2
        overall_status=1  # mark failure but continue
    fi
done

exit $overall_status
