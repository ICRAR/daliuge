Data Component Development
=================================

This section describes what developers need to do
to write a new data component that can be used
as a Data Drop during the execution of a |daliuge| graph.

Different from most other frameworks |daliuge| makes data components first class entities in the context of a workflow. In fact data components, or rather the instances of data components, the Data Drops, are driving the execution of a workflow. Consequently |daliuge| graphs are showing both application and data components as graph nodes. Edges in |daliuge| graphs are 

Types of Data Components
------------------------

|daliuge| out of the box supports five main types of Data Components.

* Posix file data components
* Memory data components
* S3 data components
* NGAS data components
* Apache Plasma data components

This range covers most of the use cases in workflows we have encountered sofar, but adding support for additional data components is possible as well. 

.. default-domain:: py

Python Data Component Class
---------------------------

Depending on the I/O model of such a new component, the development can be derived from either one of the existing higher level data components, the lower level :class:`DataIO <dlg.io.DataIO>` abstract class, or even lower with the :class:`AbstractDROP <dlg.drop.AbstractDROP>` class. The best way to get started is to checkout the code of existing data components like , e.g. the :class:`JsonDROP <dlg.json_drop.JsonDROP>` or the the :class:`S3DROP <dlg.s3_drop.S3DROP>`

I/O
---

A data components' input and output methods are defined by the abstract class :class:`DataIO <dlg.io.DataIO>`. The methods in that class are just empty definitions and have to be implemented by the actual data component.


