.. _memory_components:

Memory Components
=================

Buffer Protocol Drops
---------------------

Python buffer protocol is a special interface available for numerous python objects synonymous to
byte array syntax in C language, whereby python objects expose their raw bytes to other python
objects. Memory components exposed via the buffer protocol drop interface can benefit from
zero-copy reading and slicing by app components.

To implement :method:`AbstractDROP::buffer` it is recommended to ensure zero copying is
performed. String types in python3 use utf-8 character slicing and hence are not compatible with
buffer protocol due to characters not being of fixed size. Consider supporting encoded strings in
app drop I/O.

In Memory Drop
--------------

:class:`InMemoryDROP <dlg.drop.InMemoryDROP>` Is the simplest and most efficient data drop where data
is stored in a python binary buffer object. In memory drops are good for single threaded execution patterns
where data persistences is not needed before and after the workflow executes.

Shared Memory Drop
------------------

:class:`SharedMemoryDROP <dlg.drop.SharedMemoryDROP>`

TODO