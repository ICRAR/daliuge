* :ref:`genindex`
* :ref:`search`

.. _data_index:


Data Component Developers Guide
###########################################

This chapter describes what developers need to do to write a new data component that can be used as a Data Drop during the execution of a |daliuge| graph.

Different from most other frameworks |daliuge| makes data components first class entities in the context of a workflow. In fact data components, or rather the instances of data components, the Data Drops, are driving the execution of a workflow. Consequently |daliuge| graphs are showing both application and data components as graph nodes. Edges in |daliuge| graphs are symbolising event flow and not data flow. In fact most Data Drops just refer to their data payload using a URL. The Data Drop layer provides two main features:

* Abstraction of the I/O interface particularities of the underlying data storage mechanism.
* Implementation of the DALiuGE data state engine, which is the |daliuge| mechansim to drive the execution of the workflow graphs.

Components
==========

.. toctree::
 :maxdepth: 2

 filesystem_components
 memory_components
 ngas_components
 rdbms_components
 cloud_storage_components
 parameter_components

Custom Components
=================

.. toctree::
 :maxdepth: 2

 writing_data_components

