Concepts and Background
-----------------------

This section introduces key concepts and motivations underpinning
the |daliuge| system.

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
a correct order lies within the responsibility of the ``make`` utility. |daliuge|
follows the dataflow concept quite rigorously in that any data item along 
the directed graph can only be written by upstream tasks and, once finished, is
stricly immutable. In addition it also means that 

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

.. _dataflow.data-activated:

Data-activated
^^^^^^^^^^^^^^
In developing |daliuge|, we have extended the "traditional" dataflow
model by integrating data lifecycle management, graph execution engine, and
cost-optimal resource allocation into a coherent *data-activated* framework.
Concretely, we have made the following changes to the existing dataflow model:

* Unlike traditional dataflow models that characterise data as "tokens" moving
  across directed edges between nodes, we instead model data as the node,
  elevating them as actors who have autonomy to manage their own lifecycles and
  trigger appropriate "consumer" applications based on their own internal
  (persistent) states. In our graph model, both application (task) and data nodes
  are termed as **Drops**. What are really moving on the edge are
  :ref:`Drop Events <drop.events>`.

* While nodes/actors in the traditional dataflow are stateless functions, we
  express both computation and data nodes as stateful Drops. Statefulness not only
  allows us to manage Drops through persistent checkpointing, versioning and recovery
  after restart, etc., but also enables data sharing amongst multiple processing
  pipelines in situations like re-processing or commensal observations.
  All the state information is kept in the Drop wrapper, while the payload of the
  Drops, i.e. pipeline component algorithms and data, remain stateless.

* We introduced a small number of control flow graph nodes at the logical level
  such as *Scatter*, *Gather*, *GroupBy*, *Loop*, etc. These additional control
  nodes allow pipeline developers to systematically express complex data
  partitioning and event flow patterns based on various requirements and science
  processing goals. More importantly, we transform these control nodes into
  ordinary Drops at the physical level. Thus they are nearly transparent to the
  underlying graph/dataflow execution engine, which focuses solely on exploring
  parallelisms orthogonal to these control nodes placed by applications. In this
  way, the data-activated framework enjoys the best from both worlds - expressivity
  at the application level and flexibility at the dataflow system level.

* Finally, we differentiate between two kinds of dataflow graphs - **Logical Graph** and
  **Physical Graph**. While the former provides a higher level of
  abstraction in a resource-independent manner, the latter represents the actual
  execution plan consisting of inter-connected Drops mapped onto a given set of
  hardware resources in order to meet performance requirements at minimum cost
  (e.g. power consumption). In addition we further distinguish between **Logical Graph Templates**
  and **Logical Graphs** and **Physical Graph Templates** and **Physical Graphs**.
  The template graph in each of the pairs has a number of free parameters, while in the actual
  graph everything is fully defined. The free parameters of a **Logical Graph Template** allow
  changes to the configuration and behaviour of components, but none of those will change the
  structure and logic of the **Logical Graph**. Similarly free parameters in **Physical Graph Templates** will
  allow allocation of parts of the graph to certain hardware resources to produce the final **Physical Graph**.
  The structure of the template is the same as the structure of any **Physical Graph** derived from the same
  template. Note however that, while changing some of the parameters of a **Logical Graph Template** will not change
  the structure of the derived **Logical Graph** at all, it can dramatically change the structure of the 
  associated **Physical Graph Template** and **Physical Graph**. For example a scatter construct in a **Logical Graph Template** has 
  exactly the same strcuture for 2 and for 100,000 splits, but the **Physical Graph** will show either 2 or 100,000 branches.
