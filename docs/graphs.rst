Graphs
------

A processing pipeline in DFMS is described by a Directed Graph where the nodes
denote both task (application DROPs) and data (data DROPs). The edges denote
execution dependencies between DROPs. Section :ref:`dfms_functions` has briefly
introduced graph-based functions in DFMS. This section provides implementation
details in the DFMS prototype.

Logical Graph
^^^^^^^^^^^^^

A *logical graph* is a compact representation of the logical operation of a processing
pipeline without concerning underlying hardware resources. Such operations are
referred to as "construct" in a *logical graph*. The relationship between a DROP
and a construct resembles the one between "object and class" in OO
programming languages. In other words, most constructs are DROP templates and
multiple DROPs correspond to a single construct. Each construct has several
associated properties that users can adjust during the development of *logical graph*.
For component and data construct, execution time and data volumes are two very important
properties.

.. figure:: images/scatter_example.png

   Figure 2. An logical graph example with *Scatter*, Gather, and Group By constructs

Fig. 2 shows an example *logical graph* with several data constructs (e.g. Data1 - Data5),
component constructs (i.e. Component 1,3,4,5), and three control flow constructs
(Scatter, Gather, and Group-By).

* *Scatter* construct represents data parallelism, constructs inside a Scatter
  make up processing against a single data partition within the enclosing
  Scatter construct. A useful property of *Scatter* is *num_of_copies*.
  In this nested Scatter example in Fig. 2, if the num_of_copies for "Scatter1"
  and Scatter2 is 5 and 4 respectively, the generated physical graph
  will have in total 20 Data1/Component1/Data3 DROPs, but only 5 Component 5 DROPs
  since it is inside Scatter1 Construct but outside Scatter2.

* *Gather* construct Coming soon!

* *Group-By* construct Coming soon!

* *Loop* construct Coming soon!

Logical graphs serve as a way to describe a give algorithm or process. They are
not a suitable description of the precise execution of such process. To achieve
this, logical graphs are translated into *physical graphs*.

From Logical to Physical
^^^^^^^^^^^^^^^^^^^^^^^^

In general, we follow these steps to generate a physical graph.

* **Validity checking**. This is similar to compiler syntax error checking. For example, DFMS
  currently does not allow cyclic in the logical graph.

* **Construct unrolling**. This step "unrolls" the logical graph based on control constructs,
  creates all necessary DROPs, establishes directed edges amongst DROPs. This step
  produces the Physical Graph Template.

* **Graph partitioning**. This step uses various algorithms to partition the *physical graph
  template* to meet certain goals under some given constraints. Each logical partition
  could be scheduled onto some physical resource unit.

* **Resource mapping**. This step maps logical partitions onto a given set of resources
  in certain optimal ways (load balancing, etc.)

We have implemented the following algorithms for the graph partitioning and resource mapping steps:

* **Load balancing**. Given the available resource units (e.g. number of nodes),
  produce a partitioning scheme such that each partition has similar workload while
  the inter-node data movement is minimal.

* **Minimise pipeline execution time** while constrain the Degree of Parallelism
  (DoP, e.g. number of cores) for each virtual or physical resource unit.

* **Finish the pipeline execution on time** but using the minimum number of partitions,
  each of which has limited resource constraints (i.e. number of cores)


Physical Graph
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
