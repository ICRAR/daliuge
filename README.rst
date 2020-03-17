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

To get the latest stable version of the full package::

 pip install daliuge

If you only want the translator engine and don't need the runtime,
or vice-versa, you can install them separately::

 pip install daliuge-translator
 pip install daliuge-runtime

.. You can also install each directly from GitHub::
..
..  pip install "git+https://github.com/ICRAR/daliuge#egg=daliuge-common&subdirectory=daliuge-common"
..  pip install "git+https://github.com/ICRAR/daliuge#egg=daliuge-translator&subdirectory=daliuge-translator"
..  pip install "git+https://github.com/ICRAR/daliuge#egg=daliuge-runtime&subdirectory=daliuge-runtime"
..  pip install "git+https://github.com/ICRAR/daliuge"

Or if you plan to develop |daliuge|::

 git clone https://github.com/ICRAR/daliuge
 cd daliuge
 pip install -e daliuge-common
 pip install -e daliuge-translator # optional
 pip install -e daliuge-runtime    # optional


Porting from |daliuge| 0.X
--------------------------

With the release of |daliuge| 1.0.0
the code has been broken down into separate packages
to accommodate leaner and easier installations
when only a subset of the functionality is required.
In doing so we tried to maintain
as much backward compatibility as possible,
but there are some minor exceptions:

 * Code doing ``from dlg import delayed`` or similar must be changed
   to ``from dlg.runtime import delayed``.
 * Scripts finding the include directory path for daliuge headers
   using code like ``python -c 'import dlg; print(dlg.get_include_dir())``
   should switch to invoke ``dlg include_dir`` instead.


.. |daliuge| replace:: DALiuGE
.. _ICRAR: http://www.icrar.org
.. [#f1] 流 (pronounced Liu) is the Chinese character for "flow".
