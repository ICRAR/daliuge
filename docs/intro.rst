
.. _intro:

Introduction
============

The Data Activated 流 (Liu) Graph Engine (|daliuge|) prototype represents the **execution framework**
of the Science Data Processor (SDP) element of the Square Kilometer Array (SKA) observatory.
|daliuge| aims to provide a distributed data management platform and a
scalable pipeline execution environment to support continuous, soft real-time,
data-intensive processing for producing SKA science ready products.

The development of |daliuge| is largely based on SDP requirements, functions and the
overall architecture. Although specifically designed for SDP and SKA,
|daliuge| has adopted a generic, data-driven framework potentially applicable to
many other data-intensive applications.

|daliuge| stands on shoulders of many previous studies on dataflow, data
management, distributed systems (databases), graph theory, and HPC scheduling.
|daliuge| has also borrowed useful ideas from existing dataflow-related open
sources (mostly *Python*!) such as `Luigi <http://luigi.readthedocs.io/>`_,
`TensorFlow <http://www.tensorflow.org/>`_, `Airflow <https://github.com/airbnb/airflow>`_,
`Snakemake <https://bitbucket.org/snakemake/snakemake/wiki/Home>`_, etc.
Nevertheless, we believe |daliuge| has some unique features well suited
for data-intensive applications:

* Completely data-driven, and data DROP is the graph "node" (no longer just the edge)
  that has persistent states and events
* Integration of data-lifecycle management within the data processing framework
* Separation between logical graphs and physical graphs
* Docker-based pipeline component interface

In :doc:`overview` we give a glimpse to the main concepts present in |daliuge|.
Later sections of the documentation describe more in detail how |daliuge| works. Enjoy!
