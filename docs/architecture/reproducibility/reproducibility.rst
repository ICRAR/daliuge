.. _scientific_reproducibility:

Scientific Reproducibility
==========================

*Under construction*

The scientific reproducibility of computational workflows is a fundamental concern when conducting
scientific investigations.
Here, we outline our approach to increasing scientific confidence in DALiuGE workflows.
Modern methods create a deterministic computing environment through careful software versioning and
containerization.
We suggest testing equivalence between carefully selected provenance information to complement such
approaches.

Doing so allows any workflow system which generates identical provenance information can claim to
re-create some aspect of the original workflow execution.
Drops provide component-specific provenance information at runtime and throughout graph translation.

Additionally, a novel hash-graph (BlockDAG) method captures the relationships between components by
linking provenance throughout an entire workflow.
The resulting signature completely characterizes a workflow allowing for constant time provenance
comparison.

We refer a motivated reader to the
`related thesis <https://research-repository.uwa.edu.au/en/publications/using-blockchain-technology
-to-enable-reproducible-science>`__.


.. toctree::
 :maxdepth: 2

 rmodes
 blockdags
 adding_drops