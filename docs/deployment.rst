.. _deployment:

.. _dlg_functions:

Operational concepts
^^^^^^^^^^^^^^^^^^^^
As mentioned above, |daliuge| has been developed to enable processing of data from the future Square Kilometre Array (SKA) observatory. To support the SKA operational environment |daliuge| provides eight Graph-based functions as shown in
:numref:`dataflow.fig.funcs`. The implementation of these operational concepts in general does not restrict the usage of |daliuge| for other use cases, but it is still taylored to meet the SKA requirements.

.. _dataflow.fig.funcs:

.. figure:: images/dfms_func_as_graphs.jpg

   Graph-based Functions of the |daliuge| Prototype

The :doc:`architecture/graphs` section describes the implementation details for each function.
Here we briefly discuss how they work together to fullfill the SKA requirements.

* First of all, the *Logical Graph Template* (topleft in
  :numref:`dataflow.fig.funcs`) represents high-level
  data processing capabilities. In the case of the SKA Data Processor, they could be, for example,
  "Process Visibility Data" or "Stage Data Products".

* Logical Graph Templates are managed by *LogicalGraph Template
  Repositories* (bottomleft in :numref:`dataflow.fig.funcs`).
  The logical graph template is first selected from this repository for a specific pipeline and
  is then populated with parameters derived from the detailed description of the scheduled science observation. This generates a *Logical Graph*, expressing a workflow with resource-oblivious dataflow constructs.

* Using profiling information of pipeline components executed on specific hardware resources, |daliuge|
  then "translates" a Logical Graph into a *Physical Graph Template*, which prescribes a manifest of all Drops without specifying their physical locations.

* Once the information on resource availability (e.g. compute node, storage, etc.) is presented,
  |daliuge| associates each Drop in the physical graph template with an available resource unit
  in order to meet pre-defined requirements such as performance, cost, etc.
  Doing so essentially transforms the physical graph template into a *Physical Graph*,
  consisting of inter-connected Drops mapped onto a given set of resources.

* All four graph varieties are serializable as JSON strings, that is also how graphs are stored in repositories and transferred.

* Before an observation starts, the |daliuge| engine de-serializes a physical graph JSON string and turns all the nodes into Drop objects and then deploys all the Drops onto the allocated resources as per the
  location information stated in the physical graph. The deployment process is
  facilitated through :doc:`architecture/managers`, which are daemon processes managing the deployment of Drops
  onto the designated resources. Note that the :doc:`architecture/managers` do _not_ control the Drops or the execution, but they do monitor the state of them during the execution.

* Once an observation starts, the graph :ref:`graph.execution` cascades down the graph edges through either data Drops that triggers its next consumers or application Drops
  that produces its next outputs. When all Drops are in the **COMPLETED** state, some data Drops
  are persistently preserved as Science Products by using an explicit persist
  consumer, which very likely will be specifically dedicated to a certain
  science data product.


Deployment Scenarios
====================

The three components described in the :ref:`basics` section allow for a very flexible deployment. In a real world deployment there will always be one data island manager, zero or one master managers, and as many node managers as there are computing nodes available to the |daliuge| execution engine. In very small deployments one node manager can take over the role of the master manager as well. For extremely large deployments |daliuge| supports a hierarchy of island managers to be deployed, although even with 10s of millions of tasks we have not seen the actual need to do this. Note that the managers are only deploying the graph, the execution is completely asynchronous and does not require any of the higher level managers to run. Even the *manager functionality* of the node manager is not required at run-time.

The primary usage scenario for the |daliuge| execution engine is to run it on a large cluster of machines with very large workflows of thousands to millions of individual tasks. However, for testing and small scale applications it is also possible to deploy the whole system on a single laptop or on a small cluster. It is also possible to deploy the whole system or parts of it on AWS or a Kubernetes cluster. For instance EAGLE and/or the *translator* could run locally, or on a single AWS instance and submit the physical graph to a master manager on some HPC system. This flexible deployment is also the reason why the individual components are kept well separated. 