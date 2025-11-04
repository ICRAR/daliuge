Data Activated 流 Graph Engine
==============================

.. image:: https://github.com/ICRAR/daliuge/actions/workflows/run-unit-tests.yml/badge.svg?branch=master
   :target: https://github.com/ICRAR/daliuge/actions/workflows/run-unit-tests.yml

.. image:: https://coveralls.io/repos/github/ICRAR/daliuge/badge.svg?branch=master
    :target: https://coveralls.io/github/ICRAR/daliuge?branch=master

.. image:: https://github.com/ICRAR/daliuge/actions/workflows/create-palettes.yml/badge.svg?branch=master
   :target: https://github.com/ICRAR/daliuge/actions/workflows/create-palettes.yml

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black

.. image:: https://readthedocs.org/projects/daliuge/badge/?version=latest
    :target: https://daliuge.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/badge/License-LGPL_v2-blue.svg
    :target: https://www.gnu.org/licenses/lgpl-2.1
    :alt: License: LGPL v2.1
.. image:: https://img.shields.io/badge/ascl-1912.004-blue.svg?colorB=262255
     :target: https://ascl.net/1912.004
     :alt: ascl:1912.004

|daliuge|
is a workflow graph development, management and execution framework, specifically designed to support very large
scale processing graphs for the reduction of interferometric radio astronomy data sets.
|daliuge| has already been used for `processing large astronomical datasets 
<https://arxiv.org/abs/1702.07617>`_ in existing radio astronomy projects.
It originated from a prototyping activity as part of the `SDP Consortium
<https://www.skatelescope.org/sdp/>`_ called Data Flow Management System (DFMS). DFMS aimed to 
prototype the execution framework of the proposed SDP architecture.

Development and maintenance of |daliuge| is currently hosted at ICRAR_
and is performed by the `DIA team <http://www.icrar.org/our-research/data-intensive-astronomy/>`_.

Quickstart
==========

Most users will need to use DALiuGE locally only when prototyping their workflow as it 
is designed in EAGLE. This repository provides a `Makefile` that simplifies the setup
options for running DALiuGE locally, without needing to take into account the distributed 
and flexible nature of the ecosystem. 

.. note:: It is recommended to have Docker installed when running DALiuGE as a workflow 
prototyping tool. Please review the installation material for Docker `here <https://docs.docker.com/engine/install/>`.  

The following steps are recommended for quickstarting your DALiuGE install: 

1. Create and enter the virtual environment
2. Build the docker images
3. Run the docker images
4. Confirm the images are running and accessible from EAGLE using a web browser. 

Creating the virtual environment is as simple as running:: 

    make virtualenv
    source .venv/bin/activate 

Now, in the `.venv` environment, run:: 

    make docker-install 

This will install a development version of the DALiuGE images and is appropriate for local
installation (*not* a production environment). 

Running both the Engine and the Translator is as a simple as:: 

    make docker-run

It is possible to confirm that everything is up and running by accessing the following links in a browser
(Opera, Firefox, or Chrome are recommended):: 

    http://dlg-tm.localhost/ # Translator Manager
    http://dlg-nm1.localhost/ # Node Manager 1
    http://dlg-nm2.localhost/ # Node Manager 2
    http://dlg-dim.localhost/ # Data Island Manager


With this setup running, it is now possible to translate and deploy a prototype EAGLE workflow
on your local machine. 

For more information about the installation and usage of the system please refer to the `documentation <https://daliuge.readthedocs.io>`_


See the ``docs/`` directory for more information, or visit `our online
documentation <https://daliuge.readthedocs.io/>`_


.. |daliuge| replace:: DALiuGE
.. _ICRAR: http://www.icrar.org
.. [#f1] 流 (pronounced Liu) is the Chinese character for "flow".
