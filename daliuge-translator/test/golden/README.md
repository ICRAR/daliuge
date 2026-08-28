# Single LG to PG golden test

This directory implements the single-graph compatibility test for Issue #5.
It protects the translator wire format at three boundaries:

1. PGT after `dlg unroll`;
2. partitioned PGT (PGT-P) after `dlg partition`;
3. PG after `dlg map`.

The test intentionally does not submit or execute the graph. It uses repository
fixtures only, does not access the network, and writes candidate output only to
pytest's temporary directory.

## Pinned input and baseline

`inputs/ArrayLoop.graph` is copied without modification from
`ICRAR/EAGLE_test_repo` commit
`2f1db6c99898c43a25d9a7d3a07acf8cfb7becff`, path
`eagle_test_graphs/daliuge_tests/translator/logical_graphs/ArrayLoop.graph`.
Its SHA-256 is recorded in `manifest.json` and verified before each run. The
source repository publishes the graph under GPL-3.0.

The current legacy baseline is DALiuGE commit
`c96d83fb56d523bfcf43e061a822e960dc48a2f6`. Before replacing any expected
fixture, maintainers must confirm that this remains the intended pre-refactor
baseline. All graph source, environment, CLI options and fixture hashes are
recorded in `manifest.json`.

The fixtures were produced before adding this test. Two consecutive runs were
byte-for-byte identical for PGT, PGT-P and PG with METIS two-way partitioning.
The JSON includes the trailing reproducibility payload; the test does not remove
or broadly ignore fields.

## Run the regression test

From the repository root with the DALiuGE development environment active:

```bash
python -m pytest -q \
  daliuge-translator/test/golden/test_single_graph_golden.py
```

The runner locates `dlg` in the active environment. Set `DLG_CLI` only when the
console script is elsewhere:

```bash
DLG_CLI=/path/to/venv/bin/dlg python -m pytest -q \
  daliuge-translator/test/golden/test_single_graph_golden.py
```

Objects are compared structurally, so indentation and object-key order do not
matter. Array order, lengths, field names, JSON types and values must match. A
failure identifies the earliest stage and first differing JSON path, for
example:

```text
Stage: PGT-P
Path: $[7].node
Expected: "#0"
Actual:   "#1"
```

## Regenerate candidate fixtures

Expected files are review artifacts and must never be rebuilt automatically by
the normal test. Use an isolated worktree and virtual environment:

```bash
git worktree add ../daliuge-legacy \
  c96d83fb56d523bfcf43e061a822e960dc48a2f6
python3 -m venv ../daliuge-legacy-venv
source ../daliuge-legacy-venv/bin/activate
cd ../daliuge-legacy
python -m pip install --upgrade pip setuptools wheel
make local
```

Return to the current worktree and generate into a review directory:

```bash
PYTHONPATH=daliuge-translator python -m test.golden.generate_single_graph_golden \
  --dlg ../daliuge-legacy-venv/bin/dlg \
  --legacy-repo ../daliuge-legacy \
  --output-dir /tmp/issue5-golden-review
```

The generator verifies the legacy worktree commit and refuses to write into the
committed `expected/` directory. Review the structural diff and printed hashes
before deliberately copying PGT, PGT-P and PG into `expected/` and updating the
manifest hashes.

## Known limits

- This covers one graph, one parameter set and METIS only. Multi-graph and
  multi-configuration coverage belongs to Issue #6.
- It does not start the Engine or validate workflow business output.
- The manifest records key dependency versions but is not a complete dependency
  lock. If dependency drift affects output, reproduce the recorded environment
  before approving a baseline change.
