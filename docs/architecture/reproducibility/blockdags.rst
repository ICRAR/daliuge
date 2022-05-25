.. _reproducibility_blockdags:

Technical approach
==================

The fundamental primitive powering workflow signatures are Merkle trees and Block directed
acyclic graphs (BlockDAGs).
These data structures cryptographically compress provenance and structural information.
We describe the primitives of our approach and then their combination.
The most relevant code directory is found under ``dlg.common.reproducibility``.

Provenance data is stored internally within the graph data-structure throughout translation and execution. 

In the logical graph structure (dictionary) this information is keyed under 'reprodata'.
In the physical graph (template) structure this information is appended to the end of the droplist.

Following graph execution, the reprodata is written to a log file, alongside the associated execution logs ($DLG_ROOT/logs).

If the specified rmode is 'NOTHING', no reprodata is appended at any stage in translation and execution.


Merkle Trees
------------
A Merkle tree is essentially a binary tree with additional behaviours.
Leaves store singular data elements and are hashed in pairs to produce internal
nodes containing a signature.
These internal nodes are recursively hashed in pairs, eventually leaving a single root node with a
signature for its entire sub-tree.

Merkle tree comparisons can find differing nodes in a logarithmic number of comparisons and find
their use in version control, distributed databases and blockchains.

We store information for each workflow component in a Merkle tree.

BlockDAGs
---------

BlockDAGs are our term for a hash graph.
Each node takes the signature of a previous block(s) in addition to new information, hashes them
all together to generate a signature for the current node.
We overlay BlockDAGs onto |daliuge| workflow graphs; the edges between components remain, descendant
components receive their parents' signatures to generate their signatures, which are passed on to
their children.

The root of a Merkle tree formed by the signatures of workflow leaves acts as the full
workflow signature.

One could, in principle, do away with these cryptographic structures, but utilizing Merkle trees
and BlockDAGs make the comparison between workflow executions constant time independent of
workflow scale or composition.

Runtime Provenance
------------------

Each drop implements a series of ``generate_x_data``, where ``x`` is the name of a particular
standard (defined below).
At runtime, drops package up pertinent data then sent to its manager, percolating up to the master
responsible for the drop's session, which then packages the final BlockDAG for that workflow
execution.
The resulting signature structure is written to a file stored alongside that session's log file.

In general, specialized processing drops need to implement a customized ``generate_recompute_data``
function, and data drops need to implement a ``generate_reproduce_data`` function.

Translate-time Provenance
-------------------------
|daliuge| can generate BlockDAGs and an associated signature for a workflow at each stage of
translations from logical to physical layers.
Passing an ``rmode`` flag (defined below) to the ``fill`` operation, from that point forward,
|daliuge| will capture provenance and pertinent information automatically, storing this information
alongside the graph structure itself.

The *pertinent* information is defined in the ``dlg.common.reproducibility.reproducibility_fields``
file, which will need modification whenever an entirely new type of drop is added (a relatively
infrequent occurrence).

Signature Building
------------------
The algorithm used to build the blockDAG is a variant of
`Kahn's algorithm <https://www.geeksforgeeks.org/topological-sorting-indegree-based-solution/>`__
for topological sorting.
Nodes without predecessors are processed first, followed by their children, and so on, moving
through the graph.

This operation takes time linear in the number of nodes and edges present in the graph at all
layers.
Building the MerkleTree for each drop is a potentially expensive operation, dependent on the volume
of data present in the tree.
This is a per-drop consideration, and thus when implementing ``generate_reproduce_data``, be wary of
producing large data volumes.
