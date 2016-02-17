Concepts and Background
-----------------------

This section briefly introduces key concepts and motivations underpinning DFMS.

Dataflow
^^^^^^^^
A traditional dataflow computation model does not explicitly place any control or
constraints on the order or timing of operations beyond what is inherent in the
data dependencies among compute tasks. The removal of explicit scheduling of
compute task in the dataflow model has opened up new (e.g. parallelism)
opportunities that are previously masked by "artificial" control flow imposed by
applications or programmers. A similar example is the ``make`` tool, where the
programmer focuses on defining each target and its dependencies. The burden of
exploring parallelism to efficiently execute many individual compiling tasks in
a correct order lies within the responsibility of the Make utility.

Graph
^^^^^
Following the dataflow model, a computer program can be described by a Directed
Graph where the nodes denote compute task, and the edges denote data dependencies
between operations.  In principle, a dataflow graph consists of edges,
nodes (or actors), and tokens. Tokens represent data items and travel across
directed edges to be transformed at nodes into other data items (similar to
functions). While in theory the dataflow model provides a powerful yet simple
formalism to describe parallel computation, early efforts in developing
`dataflow architecture <http://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=48862>`_
had to introduce control flow operators (e.g.  switch and merge) and data
storage mechanism in order to put dataflow models into practice.

Data-driven
^^^^^^^^^^^
In developing the DFMS prototype, we have extended the "traditional" dataflow
model by integrating data lifecycle management, graph execution engine, and
cost-optimal resource allocation into a coherent data-driven framework.
Concretely, we have made the following changes to the existing dataflow model:

* Unlike traditional dataflow models that characterise data as "tokens" moving
  across directed edges between nodes, we instead model data as the node,
  elevating them as actors who have autonomy to manage their own lifecycles and
  trigger appropriate "consumer" applications based on their own internal
  (persistent) states. In our graph model, both application (task) and data nodes
  are termed as **DROPs**. What are really moving on the edge are **DROP Events**.

* We differentiate between two kinds of dataflow graphs - **Logical Graph** and
  **Physical Graph**. While the former provides a higher level of computation
  abstraction in a resource-independent manner, the latter represents the actual
  execution plan consisting of inter-connected DROPs mapped onto a given set of
  hardware resources in order to meet performance requirements at minimum cost
  (e.g. power consumption).

* We introduced a small number of control flow graph nodes at the logical level
  such as *Scatter*, *Gather*, *GroupBy*, *Loop*, etc. These additional control
  nodes allow pipeline developers to systematically express complex data
  partitioning and event flow patterns based on various requirements and science
  processing goals. More importantly, we transform these control nodes into
  ordinary DROPs at the physical level. Thus they are nearly transparent to the
  underlying graph/dataflow execution engine, which focuses solely on exploring
  parallelisms orthogonal to these control nodes placed by applications. In this
  way, the Data-Driven framework enjoys the best from both worlds - expressivity
  at the application level and flexibility at the dataflow system level.

DFMS Functions
^^^^^^^^^^^^^^
The DFMS prototype provides eight Graph-based functions as shown in Figure 1 below.

.. figure:: images/dfms_func_as_graphs.jpg

   Figure 1. Graph-centric Functions of the DFMS Prototype

Section :doc:`graphs` delves into each function and its implementations in detail.
Here we briefly discuss how they work together in our data-driven framework.
First of all, the *Logical Graph Template* (topleft in Fig. 1) essentially
represents high-level data processing capabilities. In the context of SDP for example, they
could be "Process Visibility Data" or "Stage Data Products". Many such templates
are managed by the *LogicalGraph Template Repository* (bottomleft in Fig. 1).
A Logical Graph Template is first selected from the LogicalGraph Template Repository
for a specific pipeline and then filled with scheduling block parameters. This generates
a *Logical Graph*, expressing a pipeline with resource-oblivious dataflow constructs.
Using profiles of pipeline components and COMP hardware resources, the DFMS prototype
then "translates" a Logical Graph into a *Physical Graph Template*.
