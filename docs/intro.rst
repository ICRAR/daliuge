
.. _intro:

Introduction
============

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

|daliuge| stands on shoulders of many previous studies on dataflow, data
management, distributed systems (databases), graph theory, and HPC scheduling.
|daliuge| has also borrowed useful ideas from existing dataflow-related open
sources (mostly *Python*!) such as `Luigi <http://luigi.readthedocs.io/>`_,
`TensorFlow <http://www.tensorflow.org/>`_, `Airflow <https://github.com/airbnb/airflow>`_,
`Snakemake <https://bitbucket.org/snakemake/snakemake/wiki/Home>`_, etc.
Nevertheless, we believe |daliuge| has some unique features well suited
for data-intensive applications:

* Completely data-activated, by promoting data :doc:`drops` to become graph "nodes" (no longer just edges)
  that have persistent states and can consume and raise events
* Integration of data-lifecycle management within the data processing framework
* Separation of concerns between logical graphs (high level workflows) and physical graphs (execution recipes)
* Flexible pipeline component interface, including Docker containers.

In :doc:`overview` we give a glimpse to the main concepts present in |daliuge|.
Later sections of the documentation describe more in detail how |daliuge| works. Enjoy!
