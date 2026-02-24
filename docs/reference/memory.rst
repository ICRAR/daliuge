.. _memory_drop: 

MemoryDROP 
==========

I/O
===
This is used to produce an input/output port. It is important to make sure this is separate from pydata.

PyData
=======

pydata is a a key attribute of the  memoryDROP; it is used as the following:

    1. Used to determine the type of data that we are storing (String or non-string)
    2. Used as a variable store for default values on the graph (e.g. pydata = 11)


For 1., this is necessary for all memory drops, regardless of if they have an input drop. For the  `pydata` attribute, it is necessary to pick an Type from the drop down list.
If a type is incorrectly selected (e.g. Object, instead of String), you will likely end up with pickled data in your outputs, or encoding issues at runtime.

For 2, it is possible to have a memory drop with no inputs, and use this to pass data to an input drop through the memory drop.
Sometimes this is useful to 'stub' out the graph; you may still be writing the PyFuncApp that goes into the memory drop, but you know what type of data output that app will produce and so you have it in the graph to test.
The order of precedence that DALiuGE will treat PyData is:

1. Data from an input application (across the edge)
2. Data set in the pydata 'Value' column of the
3. Data set in the pydata 'Default value' column.