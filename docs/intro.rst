.. _intro:

Introduction
############

The |daliuge| system can and has been used on a huge range of different size systems. It can easily be deployed on a person's laptop, across multiple personal machines as well as small and large compute clusters. That flexibility comes with some difficulty in describing how the system is intended to be used, since that is obviously dependent on the way it is deployed. This guide mainly describes the basic usage of the system as it would appear when deployed on a single machine or a small cluster.

DALiuGE Ecosystem
------------------

A key part of the DALiuGE ethos is to reduce the amount of time scientists spend transitioning existing pipelines towards a new, distributed scientific workflow. This is achieved by:

* Visualising the logical relationship between different tasks in the workflow (a **Logical Graph**);
* Providing drop-in support for a range of existing application 'formats', including Docker containers and shell applications;
* Removing the need to learn a new domain specifical language or scripting framework - existing code can be referenced directly in the graph without the need to produce intermediate file storage. 

Depending on the existing use case and software being used in the pipeline, it is entirely possible that `no additional code` needs to be written in order to create a distributed workflow that can be deployed across an entire cluster!

In order to achieve this, DALiuGE is developed as part of an ecosystem of tools that each support each stage of workflow development:

* DALiuGE Palette Generator Tool: This tool decomposes an existing Python module or library into DALiuGE Components, which are the basis of the visual logical graph;
* Editor for the Astronomical Graph Language Environment (EAGLE): EAGLE is the graphical environment in which a user uses components to draw the Logical Graph, which represents the high-level interactions of the different parts of the workflow.  
* DALiuGE: The workflow execution framework that manages the deployment and runtime demands of the pipeline. This 'translates' the Logical Graph into a complete workflow based on the runtime parameters, and the parallelization and flow constructst that are specified in the Logical Graph.

DALiuGE Ecosystem
------------------

A key part of the DALiuGE ethos is to reduce the amount of time scientists spend transitioning existing pipelines towards a new, distributed scientific workflow. This is achieved by:

* Visualising the logical relationship between different tasks in the workflow (a **Logical Graph**);
* Providing drop-in support for a range of existing application 'formats', including Docker containers and shell applications;
* Removing the need to learn a new domain specifical language or scripting framework - existing code can be referenced directly in the graph without the need to produce intermediate file storage. 

Depending on the existing use case and software being used in the pipeline, it is entirely possible that `no additional code` needs to be written in order to create a distributed workflow that can be deployed across an entire cluster!

In order to achieve this, DALiuGE is developed as part of an ecosystem of tools that each support each stage of workflow development:

* DALiuGE Palette Generator Tool: This tool decomposes an existing Python module or library into DALiuGE Components, which are the basis of the visual logical graph;
* Editor for the Astronomical Graph Language Environment (EAGLE): EAGLE is the graphical environment in which a user uses components to draw the Logical Graph, which represents the high-level interactions of the different parts of the workflow.  
* DALiuGE: The workflow execution framework that manages the deployment and runtime demands of the pipeline. This 'translates' the Logical Graph into a complete workflow based on the runtime parameters, and the parallelization and flow constructst that are specified in the Logical Graph.

Other workflow systems
-----------------------

DALiuGE Ecosystem
------------------

A key part of the DALiuGE ethos is to reduce the amount of time scientists spend transitioning existing pipelines towards a new, distributed pipeline. This is achieved by: 

* Visualising the logical relationship between different tasks in the pipeline (a **Logical Graph**);
* Providing drop-in support for a range of existing application 'formats', including Docker containers and shell applications;
* Removing the need to learn a new domain specifical language or scripting framework - existing code can be referenced directly in the graph without the need to produce intermediate file storage. 

Depending on the existing use case and software being used in the pipeline, it is entirely possible that `no additional code` needs to be written in order to create a distributed workflow that can be deployed across an entire cluster!

In order to achieve this, DALiuGE is developed as part of an ecosystem of tools that each support each stage of workflow development:

* DALiuGE Palette Generator Tool: This tool decomposes an existing Python module or library into DALiuGE Components, which are the basis of the visual logical graph;
* Editor for the Astronomical Graph Language Environment (EAGLE): EAGLE is the graphical environment in which a user uses components to draw the Logical Graph, which represents the high-level interactions of the different parts of the workflow.  
* DALiuGE: The workflow execution framework that manages the deployment and runtime demands of the pipeline. This 'translates' the Logical Graph into a complete workflow based on the runtime parameters, and the parallelization and flow constructst that are specified in the Logical Graph.

Other workflow systems
-----------------------

|daliuge| stands on shoulders of many previous studies on dataflow, data
management, distributed systems (databases), graph theory, and HPC scheduling.
|daliuge| has also borrowed useful ideas from existing dataflow-related open
sources (mostly *Python*!) such as `Luigi <http://luigi.readthedocs.io/>`_,
`TensorFlow <http://www.tensorflow.org/>`_, `Airflow <https://github.com/airbnb/airflow>`_,
`Snakemake <https://bitbucket.org/snakemake/snakemake/wiki/Home>`_, etc.
Nevertheless, we believe |daliuge| has some unique features well suited
for data-intensive applications:

* Completely :ref:`data-activated <dataflow.data-activated>`, by promoting data :doc:`architecture/drops` to become graph "nodes" (no longer just edges)
  that have persistent states and can consume and raise events
* Integration of data-lifecycle management within the data processing framework
* Separation of concerns between logical graphs (high level workflows) and physical graphs (execution recipes)
* Flexible pipeline component interface, including Docker containers.
* Native multi-core execution out of the box

In :doc:`architecture/index` we give a glimpse to the main concepts present in |daliuge|.
Later sections of the documentation describe more in detail how |daliuge| works. Enjoy!

