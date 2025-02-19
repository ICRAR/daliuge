.. _intro:

Introduction
############

The Data Activated 流 (Liu) Graph Engine (|daliuge|) is a workflow graph execution framework, 
specifically designed to support very large scale processing graphs for the reduction of 
interferometric radio astronomy data sets.
|daliuge| aims to provide a distributed data management platform and a
scalable pipeline execution environment to support continuous, soft real-time,
data-intensive processing for producing radio astronomy data products.

|daliuge| originated from a prototyping activity as part of the SKA SDP Consortium called Data Flow Management System (DFMS).

The development of |daliuge| is largely based on radio astronomy processing requirements.
However, |daliuge| has adopted a generic, data-driven framework architecture potentially applicable to
many other data-intensive applications.

DALiuGE Ecosystem
------------------

A key part of the DALiuGE ethos is to reduce the amount of time scientists spend transitioning existing pipelines towards a new, distributed pipeline. This is achieved by: 

* Visualising the logical relationship between different tasks in the pipeline (a "Logical Graph"),
* Providing drop-in support for a range of existing application 'formats', including Docker containers,
* Removing the need to learn a new domain specifical language or scripting framework - existing code can be referenced directly in the graph without the need to produce intermediate file storage. 

Depending on the existing use case and software being used in the pipeline, it is entirely possible that `no additional code` needs to be written in order to create a distributed pipeline that can be deployed across an entire cluster!

In order to achieve this, DALiuGE is developed as part of an ecosystem of tools that each support each stage of pipeline development: 

* DALiuGE Palette Generator Tool: This tool decomposes an existing Python module or library into DALiuGE Components, which are the basis of the visual logical pipeline;
* Editor for the Astronomical Graph Language Environment (EAGLE): EAGLE is the graphical environment in which a user uses components to draw the Logical Graph, which represents the high-level interactions of the different parts of the workflow.  
* DALiuGE: The workflow execution framework that manages the deployment and runtime demands of the pipeline. This 'translates' the Logical Graph into a complete workflow based on the runtime parameters provided in the Logical Graph. 

Moving Forward 
---------------
To progress with :doc:`tutorial/first_workflow`

We discuss these different projects because we will cross-reference the different tools in the initial Quickstart material for DALiuGE. If you are interested in starting to develop your own pipeline with DALiuGE and EAGLE, t is recommended to take the following steps to help progress with the tutorial example: 

* EAGLE: The editor provides 
* DALiuGE: Installation 
* dlg_paletteGen: Installation  

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

