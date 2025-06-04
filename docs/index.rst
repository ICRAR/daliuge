.. |br| raw:: html

   <br />

|daliuge|
#########

Welcome to the Data Activated 流 [#f1]_ Graph Engine (|daliuge|).

|daliuge| is a workflow graph execution framework, specifically designed to support very large scale processing graphs for the reduction of interferometric radio astronomy data sets. DALiuGE has already been used for processing large astronomical datasets in existing radio astronomy projects. It originated from a prototyping activity as part of the SDP Consortium called Data Flow Management System (DFMS). DFMS aimed to prototype the execution framework of the proposed SDP architecture.
For a complete tour of |daliuge| please read
our `overview paper <http://dx.doi.org/10.1016/j.ascom.2017.03.007>`_. DALiuGE has been used in a project running a `full-scale simulation <http://dx.doi.org/10.1109/SC41405.2020.00006>`_ of the Square Kilometre Array dataflow on the ORNL Summit supercomputer.

.. figure:: images/DALiuGE_naming_rationale.png

Development and maintenance of |daliuge| is currently hosted at ICRAR_
and is performed by the `DIA team <http://www.icrar.org/our-research/data-intensive-astronomy/>`_.

Quick Start Guide
-----------------

:doc:`/intro` |br|
:doc:`/installing` |br| 
:doc:`/running` |br|
:doc:`/basics`

DALiuGE Fundamentals
--------------------
:doc:`/architecture/index` |br|
:doc:`/deployment/overview` |br|

Using the command line interface
--------------------------------
:doc:`/cli/cli_basics`
:doc:`/cli/cli_engine`
:doc:`/cli/cli_translator`

Advanced DALiuGE Methods
------------------------
:doc:`/advanced/delayed` |br| 


API Documentation
-----------------
:doc:`api-index` |br| 
:doc:`development/app_development/app_index` |br|
:doc:`development/data_development/data_index` |br|


.. toctree::
   :maxdepth: 1
   :caption: Quickstart Guide
   :hidden: 
    
   intro
   installing
   running
   basics
 
.. toctree::
   :maxdepth: 2
   :caption: DALiuGE Fundamentals
   :hidden: 

   architecture/index
   deployment/overview
   
.. toctree:: 
   :maxdepth: 1 
   :hidden: 
   :caption: Advanced DALiuGE Methods

   advanced/delayed
   
.. toctree:: 
   :maxdepth: 1
   :hidden:
   :caption: CLI User Guide

   cli/cli_basics
   cli/cli_engine
   cli/cli_translator
   cli/cli_remote

.. toctree::
   :caption: Reference
   :maxdepth: 2
   :hidden:

   api-index
   development/template_primer
   development/app_development/app_index 
   development/data_development/data_index 

.. toctree:: 
    :hidden:

Citations
---------
As you use |daliuge| for your exciting projects, please cite the following paper:

`Wu, C., Tobar, R., Vinsen, K., Wicenec, A., Pallot, D., Lao, B., Wang, R.,
An, T., Boulton, M., Cooper, I. and Dodson, R., 2017.
DALiuGE: A Graph Execution Framework for Harnessing the Astronomical Data Deluge.
Astronomy and Computing, 20, pp.1-15. (2017) <https://arxiv.org/pdf/1702.07617.pdf>`_

.. _ICRAR: http://www.icrar.org
.. [#f1] 流 (pronounced Liu) is the Chinese character for "flow".
