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

String Serialization
--------------------

Raw String
""""""""""

The simplest serialization format supported directly by `DataDrop.write` and `DataDrop.read`.

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
using `dicttoxml` or both serialization and deserialiation using `xml.etree.ElementTree`.


Binary Serialization
--------------------

Data drops specify the location the I/O interface reads and writes to but the app drop
may decide the type of binary format to write.

Pickle (.pkl)
"""""""""""""

Default serialazation format. Use `save_pickle` for serialization to this format and 
`allDropContents` or `load_pickle` for deserialization.


Numpy (.npy)
""""""""""""

Portable numpy serialization format. Use `save_numpy`

Numpy Zipped (.npz)
"""""""""""""""""""

Portable zipped numpy serialization format. Consists of a .zip directory holding one or more .npy
files.


Table Serialization
-------------------

For certain drops such as RDBMSDrop these drops serialize table data in the form of dataframe
objects.
