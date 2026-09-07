# EAGLE graph corpus (Phase 0 test fixtures)

Logical graphs copied from [`ICRAR/EAGLE-graph-repo`](https://github.com/ICRAR/EAGLE-graph-repo)
at commit `829e3efc6dc7f86b79c12ec4381f24c72f30f4a8`, pinned here so future changes to that repo
can't silently change what Phase 0 tests against.

Surveyed all 121 `.graph` files against the bare CLI build (`dlg fill ->
unroll -> partition`, using `metis` for partitioning). 84 run
cleanly and are included as-is. The rest were excluded; of those, 6
are kept anyway because they're genuinely broken graphs (not a dependency
problem) and are useful as known-bad fixtures:

| Graph | Known issue |
|---|---|
| `ms_transform_bash.graph` | Command template contains a literal `{`/`}` that the CLI's string formatting misreads as a placeholder (`ValueError: Invalid placeholder in string`). |
| `chilies_daliuge_split.graph` | Same class of issue -- invalid placeholder in a command template string. |
| `casda_download_pipeline_laptop_ABC.graph` | Same class of issue -- invalid placeholder in a command template string. |
| `advent_simple.graph` | Graph file is missing the `nodeDataArray` field the parser expects -- looks like an old/incompatible file format. |
| `LEAP-SDP-STREAMING.graph` | A Gather node's input link points at a node that doesn't resolve (`GInvalidLink`) -- broken link topology. |
| `LeapAccelerateCLI.graph` | Contains a dangling reference to a node UUID that isn't present in the graph. |

The remaining excluded graphs are not included here -- they fail because
they need app-specific components not present in the bare CLI build
(mostly RASCIL/casacore-style pipelines hitting `KeyError: 'id'`, plus a
few needing `astropy`/`numpy.recarray` components). Not a bug, just out of
scope for this bare-CLI test corpus.

Partitioning: `metis` was used (needs a natively compiled `libmetis` -- on
macOS, `brew install metis` then `export METIS_DLL=$(brew --prefix
metis)/lib/libmetis.dylib`). See issue #4 discussion for why `metis` was
chosen over `mysarkar`.
