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
""""""""""""""""""""
Each construct has several
associated properties that users have control over during the development of *logical graph*.
For Component and Data construct, **Execution time** and **Data volume** are two very important
properties. Such properties can be directly obtained from parametric models or
`estimated <http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=546196>`_ from the profiling information (e.g. pipeline component workload characterisation) and COMP platform specification.

Control flow constructs
"""""""""""""""""""""""
Control flow constructs form the "skeleton" of the *logical graph*, and determine
the final structure of the *physical graph* to be generated. DFMS currently supports
the following flow constructs:

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
  in the order of ``[outer_partition_id][inner_partition_id]`` are resorted as ``[inner_partition_id][outer_partition_id]``.
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
     a continuous imaging pipeline. This example can be `viewed online <http://sdp-dfms.ddns.net/lg_editor?lg_name=cont_img.json>`_ in the DFMS prototype.

Repository
""""""""""
The DFMS prototype uses a Web-based *logical graph* editor as the default user interface
to the underlying *logical graph repository*, which currently is simply a managed
POSIX file system directory. Each logical graph is physically stored as a
JSON-formatted textual file, and can be accessed and modified remotely through
the *logical graph* editor via the RESTful interface. For example, the JSON file for the continuous
imaging pipeline as shown partially in Fig. 3. can be accessed `through HTTP GET <http://sdp-dfms.ddns.net/jsonbody?lg_name=cont_img.json>`_.
The editor also provides a Web-based JSON editor so that users can directly change
the graph JSON content inside the repository.


Select template
"""""""""""""""
While the DFMS *logical graph* editor does not differentiate between *logical graph*
and *logical graph template*, users can create either of them using the editor. After all,
the only differences between these two are the populated values for some parameters.
Once a template is created or selected, users can simply copy and paste the JSON content into
the new *logical graph* and fill in those parameter values (as construct properites)
using the editor. Note that the public version of the *logical graph* editor has
not yet opened its "create new logical graph" API.


Translation
^^^^^^^^^^^
While a *logical graph* provides a compact way to express complex processing logic,
it contains high level control flow specifications that are not directly usable
by the underlying graph execution engine and DROP managers. To achieve that,
logical graphs are translated into *physical graphs*. The translation process essentially
creates all DROPs and is implemented in the :doc:`api/dropmake` module.

Basic steps
"""""""""""
**DropMake** in the DFMS prototype involves the following steps:

* **Validity checking**. Checks whether the *logical graph* is ready to be translated.
  This step is similar to semantic error checking used in compilers.
  For example, DFMS currently does not allow any cycles in the logical graph. Another
  example is that *Gather* can be placed only after a *Group By* or a *Data* construct
  as shown in Figure 2. Any validity errors
  will be displayed as exceptions on the *logical graph* editor.

* **Construct unrolling**. Unrolls the *logical graph* by (1) creating all necessary DROPs
  (including "artifact" DROPs that do not appear in the original *logical graph*),
  and (2) establishing directed edges amongst all newly generated DROPs. This step
  produces the **Physical Graph Template**.

* **Graph partitioning**. Decomposes the *Physical Graph Template* into a set of
  logical partitions (a.k.a. *DropIsland*) and generates an order of DROP
  execution sequence within each partition such that certain performance
  requirements (e.g. total completion time, total data movement, etc.) are met
  under given constraints (e.g. resource footprint).
  This step produces the **Physical Graph Template Partition**.

* **Resource mapping**. Maps each logical partition onto a given set of resources
  in certain optimal ways (load balancing, etc.). This steps requires
  near real-time resource usage information from the COMP platform or the Local Monitor & Control (LMC).
  It also needs DROP managers to coordinate the DROP deployment.
  In some cases, this mapping step is merged with the previous *Graph partitioning* step
  to directly produce DROPs to resource mapping.

Algorithms
""""""""""
Scheduling an Acyclic Directed Graph (DAG) that involves graph partitioning and resource mapping as stated in `Basic steps`_
is known to be an `NP-hard problem <http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=210815>`_.
The DFMS prototype has tailored several heuristics-based algorithms from previous research on `DAG scheduling <http://dl.acm.org/citation.cfm?id=344618>`_
and `graph partitioning <http://www.sciencedirect.com/science/article/pii/S0743731597914040>`_ to perform these two steps. These algorithms are currently configured by DFMS to utilise uniform hardware resources.
Support for heterogenous resources using the `List scheduling <https://en.wikipedia.org/wiki/List_scheduling>`_
algorithm will be made available shortly. With these algorithms, the DFMS prototype
currently can deal with the following translation / mapping problems:

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

Deployment
""""""""""
Use the client.
