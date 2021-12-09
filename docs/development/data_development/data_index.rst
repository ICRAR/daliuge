* :ref:`genindex`
* :ref:`search`

.. _data_index:


|daliuge| *Data* Component Developers Guide
###########################################

This chapter describes what developers need to do to write a new data component that can be used as a Data Drop during the execution of a |daliuge| graph.

Different from most other frameworks |daliuge| makes data components first class entities in the context of a workflow. In fact data components, or rather the instances of data components, the Data Drops, are driving the execution of a workflow. Consequently |daliuge| graphs are showing both application and data components as graph nodes. Edges in |daliuge| graphs are symbolising event flow and not data flow. In fact most Data Drops just refer to their data payload using a URL. The Data Drop layer provides two main features:

* Abstraction of the I/O interface particularities of the underlying data storage mechanism.
* Implementation of the DALiuGE data state engine, which is the |daliuge| mechansim to drive the execution of the workflow graphs.

.. default-domain:: py

Types of Data Components
------------------------
|daliuge| out of the box includes the following Data Components:

#. Posix file data component
#. Posix directory component
#. Memory data component
#. Shared memory data component
#. S3 data component
#. NGAS data component
#. Apache Plasma and Plasma Flight data Components
#. RDBMS component

This range covers most of the use cases in workflows we have encountered sofar, and thus in most cases it is likely not required to develop additional ones. However, adding a new data component is still possible and briefly outlined here. Depending on the I/O model of such a new component, the development can be derived from either one of the existing higher level data components, the lower level :class:`DataIO <dlg.io.DataIO>` abstract class, or even lower with the :class:`AbstractDROP <dlg.drop.AbstractDROP>` class. The best way to get started is to checkout the code of existing data components like , e.g. the :class:`JsonDROP <dlg.json_drop.JsonDROP>` or the the :class:`S3DROP <dlg.s3_drop.S3DROP>`.

I/O
---
A data components' input and output methods are defined by the abstract class :class:`DataIO <dlg.io.DataIO>`. The methods in that class are just empty definitions and have to be implemented by the actual data component.


*NOTE: The DCDG is work in progress!*

