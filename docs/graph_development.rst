Graph development
=================

.. default-domain:: py

This section describes the different ways
users can develop workflows (either Logical or Physical)
to work with |daliuge|.

As explained in :doc:`architecture/graphs`,
|daliuge| describes computations
in terms of Directed Graphs.
Two different classes of graphs are used
in the |daliuge| workflow development:

#. *Logical Graphs*, a high-level, compact representation of the application logic. Logical Graphs are directed graphs, but not acyclic.
#. *Physical Graphs*, a detailed description of each individual processing step. Physical Graphs are Directed Acyclic Graphs (DAG)

When submitting a graph for execution, the |daliuge| engine expects a |pg|. Therefore a |lg| needs to be first translated into a |pg| before submitting it for execution.
The individual steps that occur during this translation process are detailed in :ref:`graphs.translation`.

The following graph development techniques are available for users to creates graphs and submit them for execution:

.. _graph_dev.lge:

Using the Logical Graph Editor EAGLE
------------------------------------

Please refer to the `EAGLE documentation <https://eagle-dlg.readthedocs.io>`__ for detailed information. When using the EAGLE graph editor the translator and engine levels are not really exposed to the user, thus in the following we will describe a few examples of how to directly generate a graph.

.. _graph_dev.pg:

