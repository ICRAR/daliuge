.. _writing_data_components:

Writing Data Components
=======================

Data I/O
--------

A data components' input and output methods are defined by the abstract class :py:class:`DataIO <dlg.io.DataIO>`. The methods in that class are just empty definitions and have to be implemented by the actual data component.

*NOTE: The Daliuge Component Developement Guide is work in progress!*

Graph Translation
-----------------

DALiuGE uses :py:class:`dlg.common.Categories`, :py:data:`dlg.common.STORAGE_TYPES` and :py:data:`dlg.graph_loader.STORAGE_TYPES` to serialize and deserialize logical graph data drops. These must
be extended for new data drop types to be processed by the translator. 

*NOTE: The Daliuge Component Developement Guide is work in progress!*

Eagle Data Integeration
-----------------------

Eagle can be extended with custom data drops.

*NOTE: The Daliuge Component Developement Guide is work in progress!*
