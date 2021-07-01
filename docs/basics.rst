.. _basics:

Basics
======

The |daliuge| system consists of three main components: 

    * the EAGLE visual workflow editor,
    * the translator, partitioning and scheduling service (short just *Translator*) and
    * the execution engine (*Engine* for short).

Each of these components can be deployed, run and used independently. The most convenient way of using the system is to drive it from EAGLE. EAGLE is a web application, while the *Translator* service as well as the execution *Engine* services expose RESTful interfaces, which can be used programatically or using command line tools like *curl*. In addition there is a web monitoring tool exposed by the execution engine available as well. 

The *Editor* and the *Translator* are fairly lightweight and don't require a lot of resources. In particular the editor can be deployed locally on a user's laptop, but there is also a version running under https://eagle.icrar.org. 

The |daliuge| execution engine *can* be run on a laptop as well, but, other than for testing, there is no real good use case to do this. More realistic deployment platforms are large High Performance Computing (HPC) clusters or dynamically scalable environments such as the AWS Elastic Computing Service (ECS) or a Kubernetes cluster. All three components of |daliuge| can be installed and run natively or in docker containers. 

EAGLE
#####

EAGLE is a web-application allowing users to develop complex scientific workflows using a visual paradigm similar to Kepler and Taverna or Simulink and Quartz composer. It also allows groups of scientists to work together on such workflows, version them and prepare them for execution. A workflow in |daliuge| terminology is represented as a graph. In fact the |daliuge| system is dealing with a whole set of different graphs representing the same workflow, depending on the lifecycle state of that workflow. EAGLE just deals with the so-called *Logical Graph* state of a given workflow. EAGLE also offers an interface to the *Translator* and, through the *Translator* also to the *Engine*. For detailed information about EAGLE please refer to the EAGLE basic documentation under https://github.com/ICRAR/EAGLE as well as the `detailed usage documentation in readthedocs <https://eagle-dlg.readthedocs.io>`__.

Translator service
##################

The Translator, partitioning and scheduling service is a RESTful service. It takes a *Logical Graph* representation of a workflow and translates that into a *Physical Graph*, which in turn is a directed acyclic graph (DAG). It then uses that DAG and applies some complex heuristic algorithms to distribute the complete DAG on the available platform in an optimized way and also produces an optimzed schedule for that distribution. While the *Logical Graph* might look quite small, it can easily translate into a *Physical Graph* with thousands or millions of nodes and optimizing even just the placement of the nodes of such a system represents a N-P hard problem. The *Physical Graph* can then be send to the *Execution Engine* for execution.


Execution Engine
################

The *Engine* consists of three kinds of RESTful services in order to be able to deal with very large *Physical Graphs* produced by the *Translator*:

    * Master manager
    * Data Island manager
    * Node manager
 
In addition there is also a small web application, which allows to monitor the progress of |daliuge| execution sessions. The managers are only involved in the deployment of the *Physical Graph*, the execution, once started does not require any central control. During runtime the managers are just monitoring the progress. They also allow to stop or terminate a running workflow. During deployment the master manager uses the partitioning information produced by the *Translator* to split up the *Physical Graph* and send those partitions to node managers. In general each partition will be send to a different compute node, but that is not a rigid mapping. 