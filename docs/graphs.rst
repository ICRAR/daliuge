Graphs
------

In order to perform some execution in the system, it must first be represented
in the form of a graph. Graphs contain both tasks and data, which are both
represented by nodes on the graph.

Logical graph
^^^^^^^^^^^^^

A *logical graph* is a compact representation of the logical steps of a process.
In particular, parallelism is represented in a simplified way, describing only
the elements that make up a single branch after a given split.

Logical graphs serve as a way to describe a give algorithm or process. They are
not a suitable description of the precise execution of such process. To achieve
this, logical graphs are translated into *physical graphs*.

Physical graph
^^^^^^^^^^^^^^

A *physical graph* is a collection of inter-connected DROPs representing an
execution plan. The nodes of a physical graph are DROPs representing either
data or applications.

Edges on the graph always run between a data DROP and an application DROP. This
establishes a set of reciprocal relationships between DROPs:

* A data DROP is the *input* of an application DROP; on the other hand
  the application is a *consumer* of the data DROP.
* Likewise, a data DROP can be a *streaming input* of an application
  DROP (see :ref:`drop.relationships`) in which case the application is seen as
  a *streaming consumer* from the data DROP's point of view.
* Finally, a data DROP can be the *output* of an application DROP, in
  which case the application is the *producer* of the data DROP.

Physical graphs are the final product fed into the :ref:`drop.managers`. The
fact that they contain DROPs means that they describe exactly what an execution
consists of. They also contain partitioning information that allows the
different managers to distribute them across different nodes and Data Islands.
