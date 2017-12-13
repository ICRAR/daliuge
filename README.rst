Data Activated 流 Graph Engine
==============================

.. image:: https://travis-ci.org/ICRAR/daliuge.svg?branch=master
    :target: https://travis-ci.org/ICRAR/daliuge

.. image:: https://coveralls.io/repos/github/ICRAR/daliuge/badge.svg?branch=master
    :target: https://coveralls.io/github/ICRAR/daliuge?branch=master

.. image:: https://readthedocs.org/projects/daliuge/badge/?version=latest
    :target: https://daliuge.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

|daliuge|
is a workflow graph execution framework, specifically designed to support very large
scale processing graphs for the reduction of interferometric radio astronomy data sets.
|daliuge| has already been used for `processing large astronomical datasets 
<https://arxiv.org/abs/1702.07617>`_ in existing radio astronomy projects.
It originated from a prototyping activity as part of the `SDP Consortium
<https://www.skatelescope.org/sdp/>`_ called Data Flow Management System (DFMS). DFMS aimed to 
prototype the execution framework of the proposed SDP architecture.


Development and maintenance of |daliuge| is currently hosted at ICRAR_
and is performed by the `DIA team <http://www.icrar.org/our-research/data-intensive-astronomy/>`_.

See the ``docs/`` directory for more information, or visit `our online
documentation <https://daliuge.readthedocs.io/>`_


Installation
------------

To get the latest stable version::

 pip install --process-dependency-links daliuge

Otherwise clone this repository, go inside,
and run either ``pip install --process-dependency-links .`` (preferred)
or ``python setup.py install``.

.. |daliuge| replace:: DALiuGE
.. _ICRAR: http://www.icrar.org
.. [#f1] 流 (pronounced Liu) is the Chinese character for "flow".
