.. _memory_components:

Memory Components
=================

In Memory Drop
--------------

:class:`InMemoryDROP <dlg.drop.InMemoryDROP>` Is the simplest and most efficient data drop where data
is stored in a python binary buffer object. In memory drops are good for single threaded execution patterns
where data persistences is not needed before and after the workflow executes.

Shared Memory Drop
------------------

:class:`SharedMemoryDROP <dlg.drop.SharedMemoryDROP>` Is a special in-memory drop that allows
multiple processes to read from the same memory location. See :ref:`graph.shared_memory` for
further information.

Buffer Protocol
---------------

Python buffer protocol is a special interface available for numerous python objects synonymous to
byte array syntax in C language, whereby python objects expose their raw bytes to other python
objects. Memory components exposed via the buffer protocol drop interface can benefit from
zero-copy reading and slicing by app components.

To implement :py:meth:`DataIO.buffer <dlg.io.DataIO.buffer>` it is recommended to ensure zero copying is
performed with memory based drops for greatly improved drop read performance.

Note: String types in python3 use utf-8 character slicing and hence are not compatible with
buffer protocol due to characters not being of fixed size. Consider supporting encoded strings in
app drop I/O.