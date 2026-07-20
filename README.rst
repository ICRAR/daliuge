Data Activated 流 Graph Engine
==============================
.. image:: https://img.shields.io/pypi/v/daliuge-common.svg?maxAge=86400
.. image:: https://img.shields.io/pypi/pyversions/daliuge-common.svg?maxAge=86400

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

1. Build the docker images
2. Run the docker images
3. Confirm the images are running and accessible from EAGLE using a web browser. 

To build the docker images run:: 

    make docker-install 

This will install a development version of the DALiuGE images and is appropriate for local
installation (*not* a production environment). 

Running both the Engine (with one island manager and two node managers) and the Translator is as a simple as:: 

    make docker-start

It is possible to confirm that everything is up and running by accessing the following links in a browser
(Opera, Firefox, or Chrome are recommended):: 

    http://dlg-tm.localhost/ # Translator Manager
    http://dlg-nm1.localhost/ # Node Manager 1
    http://dlg-nm2.localhost/ # Node Manager 2
    http://dlg-dim.localhost/ # Data Island Manager

There is one additional container running which is providing a proxy gateway and name resolution into the container network::

   http://traefik.localhost

On the command line this can also be verified like this::

   ❯ docker ps
   CONTAINER ID   IMAGE                    COMMAND                  CREATED         STATUS         PORTS                                                                              NAMES
   d845df25ba92   traefik:v3.6.4           "/entrypoint.sh --lo…"   4 minutes ago   Up 4 minutes   0.0.0.0:80->80/tcp, [::]:80->80/tcp, 0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp   traefik
   be9dde120797   icrar/dlg_full:latest    "dlg dim -H 0.0.0.0 …"   4 minutes ago   Up 4 minutes   5555/tcp, 6666/tcp, 8000-8002/tcp, 9000/tcp                                        dlg-dim
   ab14ff4463ec   icrar/dlg_full:latest    "dlg daemon --tm"        4 minutes ago   Up 4 minutes   5555/tcp, 6666/tcp, 8000-8002/tcp, 9000/tcp                                        dlg-tm
   f50b2a6de075   icrar/dlg_full:latest    "dlg nm -H 0.0.0.0 -…"   4 minutes ago   Up 4 minutes   5555/tcp, 6666/tcp, 8000-8002/tcp, 9000/tcp                                        dlg-nm1
   5db3f6e31a88   icrar/dlg_full:latest    "dlg nm -H 0.0.0.0 -…"   4 minutes ago   Up 4 minutes   5555/tcp, 6666/tcp, 8000-8002/tcp, 9000/tcp                                        dlg-nm2


With this setup running, it is now possible to translate and deploy a prototype EAGLE workflow
on your local machine.

Make sure it is accessible from EAGLE
=====================================

Open https://eagle.icrar.org and navigate to the Settings by clicking on the cog wheel in the upper right corner. Go to the External Services tab and enter the translator URL::

   http://dlg-tm.localhost/gen_pgt

That will enable EAGLE to submit a graph to the translator.

Quickstop
=========

Stopping the system again can simply be done by running::

    make docker-stop

For more information about the installation and usage of the system please refer to the `documentation <https://daliuge.readthedocs.io>`_


See the ``docs/`` directory for more information, or visit `our online
documentation <https://daliuge.readthedocs.io/>`_


.. |daliuge| replace:: DALiuGE
.. _ICRAR: http://www.icrar.org
.. [#f1] 流 (pronounced Liu) is the Chinese character for "flow".
