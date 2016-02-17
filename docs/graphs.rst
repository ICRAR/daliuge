Graphs
------

A processing pipeline in DFMS is described by a Directed Graph where the nodes
denote both task (application DROPs) and data (data DROPs). The edges denote
execution dependencies between DROPs. Section :ref:`dfms_functions` has briefly
introduced graph-based functions in DFMS. This section provides implementation
details in the DFMS prototype.

Logical Graph
^^^^^^^^^^^^^

A *logical graph* is a compact representation of the logical operations in a processing
pipeline without concerning underlying hardware resources. Such operations are
referred to as **construct** in a *logical graph*. The relationship between a DROP
and a *construct* resembles the one between "object and class" in OO
programming languages. In other words, most constructs are DROP templates and
multiple DROPs correspond to a single construct.

.. figure:: images/scatter_example.png

   Figure 2. An example of *logical graph* with data constructs (e.g. Data1 - Data5),
   component constructs (i.e. Component 1,3,4,5), and control flow constructs
   (Scatter, Gather, and Group-By). This example can be viewed
   `online <http://sdp-dfms.ddns.net/lg_editor?lg_name=lofar_cal.json>`_ in the DFMS prototype.

Construct properties
""""""""""""""""""
Each construct has several
associated properties that users have control over during the development of *logical graph*.
For Component and Data construct, **Execution time** and **Data volume** are two very important
properties. Such properties can be directly obtained from parametric models or
`estimated <http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=546196>`_ from the profiling information (e.g. pipeline component workload characterisation) and COMP platform specification.

Control flow constructs
"""""""""""""""""""""""
Control flow constructs form the "skeleton" of the *logical graph*, and determine
the final structure of the *physical graph* to be generated. DFMS currently supports
four control flow constructs:

* **Scatter** indicates data parallelism. Constructs inside a *Scatter*
  represent a group of components consuming a single data partition within the enclosing
  *Scatter*. A useful property of *Scatter* is *num_of_copies*.
  In this nested Scatter example in Fig. 2, if the num_of_copies for *Scatter1*
  and *Scatter2* is 5 and 4 respectively, the generated physical graph
  will have in total 20 Data1/Component1/Data3 DROPs, but only 5 DROPs for construct "Component 5",
  which is inside *Scatter1* Construct but outside *Scatter2*.

* **Gather** indicates data barriers. Constructs inside a *Gather* represent a group
  of component consuming a sequence of data partitions as a whole. *Gather* has a property
  named "num_of_inputs", which is essentially the Gather "width" stating how many
  partitions each *Gather* instance (translated into a *BarrierAppDROP*, see :ref:`drop.execution`)
  can handle. This in turn is used by DFMS to determine how many *Gather* instances should be
  generated in the *physical graph*. *Gather* sometimes can be used in conjunction with
  *Group By* (see middle-right in Fig.2), in which case, data held in a sequence of groups are processed
  together by components enclosed by *Gather*.

* **Group By** indicates data resorting (e.g. `corner turning <https://mnras.oxfordjournals.org/content/410/3/2075.full>`_ in radio astronomy).
  The semantic is analogous to "Group By" used in SQL statement for relational
  databases, but only applicable to data DROPs. Current DFMS prototype requires *Group By* used in
  conjunction with nested *Scatter* such that data DROPs that are originally sorted
  in the order of [outer_partition_id][inner_partition_id] are now resorted as [inner_partition_id][outer_partition_id].
  In terms of parallelism, *Group By*
  is comparable to `"static" MapReduce <http://openmymind.net/2011/1/20/Understanding-Map-Reduce/>`_,
  where the keys used by all Reducers are known a prior.

* **Loop** indicates iterations. Constructs inside a *Loop* represent a group of
  components and data that will be repeatedly executed / produced for a fixed number of
  times. Given the basic DROP principle of "writing once, read many times", current
  DFMS prototype does not support dynamic branch condition for *Loop*.
  Instead, each *Loop* construct has a property named "num_of_iterations" that must be
  determined during *logical graph* development. In other words, a "num_of_iterations"
  number of DROPs for each construct inside a *Loop* will be statically generated
  in the *physical graph*.

  .. figure:: images/loop_example.png

     Figure 3. A nested-Loop (minor and major cycle) example of logical graph for
     continuous imaging pipeline. This example can be `viewed online <http://sdp-dfms.ddns.net/lg_editor?lg_name=cont_img.json>`_ in the DFMS prototype.

Logical graphs serve as a way to describe a give algorithm or process. They are
not a suitable description of the precise execution of such process. To achieve
this, logical graphs are translated into *physical graphs*.

Translation
^^^^^^^^^^^

DFMS follows the following steps to generate a *physical graph*.

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

The DFMS prototype has tailored `DAG scheduling <http://dl.acm.org/citation.cfm?id=344618>`_
and `graph partitioning <http://www.sciencedirect.com/science/article/pii/S0743731597914040>`_
algorithms, all of which are currently configured to utilise uniform hardware resources.
Support for heterogenous resources using the `List scheduling <https://en.wikipedia.org/wiki/List_scheduling>`_
algorithm will be made available shortly.

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
