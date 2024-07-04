.. _datadrop_io:

DataDROP I/O
============

An application's input and output drops
are accessed through its
:class:`inputs <dlg.drop.AppDROP.inputs>` and
:attr:`outputs <dlg.drop.AppDROP.outputs>` members.
Both of these are lists of :class:`drops <dlg.drop.AbstractDROP>`,
and will be sorted in the same order
in which inputs and outputs
were defined in the Logical Graph.
Each element can also be queried
for its :attr:`uid <dlg.drop.AbstractDROP.uid>`.

Data can be read from input drops, and written in output drops.
To read data from an input drop, one calls first the drop's
:attr:`open <dlg.drop.DataDROP.open>` method, which returns a descriptor to the opened drop.
Using this descriptor one can perform successive calls to
:attr:`read <dlg.drop.DataDROP.read>`, which will return the data stored in the drop.
Finally, the drop's :attr:`close <dlg.drop.DataDROP.close>` method
should be called to ensure that all internal resources are freed.

Writing data into an output drop is similar but simpler. Application authors need only call
one or more times the :attr:`write <dlg.drop.DataDROP.write>` method
with the data that needs to be written.

Serialization
-------------

Many data components are capable of storing data in multiple formats determined by the drop component. The common data io interface allows app components to be compatible with many data component types, however different app components connected to the same data component must use compatible serialization and deserialization types and utilities.

String Serialization
^^^^^^^^^^^^^^^^^^^^

Raw String
""""""""""

The simplest deserialization format supported directly by `DataDrop.write` and `DataDrop.read`.

JSON (.json)
""""""""""""

Portable javascript object format encoded in utf-8. JSON Schema is to be handled by the input and
output apps, which may also be stored as JSON. Serialization of python dictionaries is provided by 
`json.dump` and deserialization with `json.load`.

INI (.ini)
""""""""""

Simple format for storing string key-value pairs organized by sections that is supported by the python
`configparser` library. Due to the exclusive use of string types this format is a good for mapping directly to
command line arguments.

YAML (.yaml)
""""""""""""

Markup format with similar featureset to JSON but additionally contains features such as comments, anchors and
aliases which make it more human friendly to write. Serialization of dictionaries is provided by `yaml.dump`
and deserialization with `yaml.load`.

XML (.xml)
""""""""""

Markup format with similar features to YAML but with the addition of attributes. Serialization can be performed 
using `dicttoxml` or both serialization and deserialization using `xml.etree.ElementTree`.


Python Eval (.py)
"""""""""""""""""

Python expressions and literals are valid string serialization formats whereby the string data is iterpreted as python code. Serialization is typically performed using the `__repr__` instance method and deserialization using `eval` or `ast.eval_literal`.

Binary Serialization
^^^^^^^^^^^^^^^^^^^^

Data drops may also store binary formats that are typically more efficient than string formats
and may utilize the python buffer protocol.

Raw Bytes
"""""""""

Data drops can always be read as raw bytes using `droputils.allDropContents` and written to using `DataDROP.write`. Reading as a bytes object creates a readonly in-memory data copy that may not be as performant as other drop utilities.

Pickle (.pkl)
"""""""""""""

Default serialazation format capable of serializing any python object. Use `save_pickle` for serialization to this format and `load_pickle` for deserialization.

Numpy (.npy)
""""""""""""

Portable numpy serialization format. Use `save_numpy` for serialization and `load_numpy` for deserialization.

Numpy Zipped (.npz)
"""""""""""""""""""

Portable zipped numpy serialization format. Consists of a .zip directory holding one or more .npy
files.

Table Serialization
^^^^^^^^^^^^^^^^^^^

parquet (.parquet)
"""""""""""""""""""

Open source column-based relational data format from Apache.

Specialized Serialization
^^^^^^^^^^^^^^^^^^^^^^^^^

Data drops such as RDBMSDrop drops manage their own record format and are
interfaced using relational data objects such `dict`, or `pandas.DataFrame`.