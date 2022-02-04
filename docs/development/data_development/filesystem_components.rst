.. _filesystem_components:

Filesystem Components
=====================

Path Based Drop
---------------

:class:`PathBasedDrop <dlg.drop.PathBasedDrop>` is an interface for retreiving the path for drops that are backed by a
filesystem such as local filesystem, NFS or MPFS. Many libraries either have or only have support for reading and writing
with a filesystem path.

File Drop
---------

:class:`FileDROP <dlg.drop.FileDROP>` is a highly compatibile data drop type that can be easily used as persistent volume I/O
and inspection of individual app component I/O. The downside of using file drops is reduced I/O performance compared to
alternative memory based drops that can instead utilize buffer protocol.

Container Drop (deprecated)
---------------------------

:class:`ContainerDROP <dlg.drop.ContainerDROP>`

Directory Container Drop (deprecated)
-------------------------------------

:class:`DirectoryContainer <dlg.drop.DirectoryContainer>`

Using Filesystem Components as Persistent Volume Storage
========================================================

Filesystem component paths are relative to the temporary DALiuGE workspace but may also be of absolute path to the
the machine running DALiuGE engine of connected app drops, most commonly the default DALiuGE shared filesystem mount location
'/tmp/dlg/workspace` located on both the host machine and daliuge engine virtual machine.

In the following example graph, both the input.txt file drop and output.txt file drop paths point to a persistent absolute location
in the workspace folder. On successful graph execution, the output location will be updated.

.. image:: img/filedrop_sample_1.png

Depending on DALiuGE settings, the relative workflow path will also be generated and made persistent. Setting the output path to a relative
location will populate a new workflow directory with output after every successful execution.

.. image:: img/filedrop_sample_2.png

