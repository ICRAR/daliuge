.. _supported_encoding


Native supported formats
########################

Dill & pickle
-------------
DALiuGE supports reading pickled data, using the ``dill`` and ``pickle`` modules to read this information. It is expected pickled data will be deserialised and usable without needing additional transformation.

The DALiUGE engine using pickle as the default encoding. 

UTF-8
-------
DALiuGE will read and write UTF-8-encoded string data. Any transformations of this data must be processed by the consumer AppDRROP; for example, .csv data transformed into data tables. 

.. note:: 
    Writing non-string data as UTF-8 may result in data that is poorly formatted and not easily read back in; for example, writing data that is a NumPy array or pandas DataFrame as UTF-8 will produce the ``repr``


NumPy
-----
Native support for ``.npy`` files is provided and requires no additional post-processing once consumed by AppDROPs. 

Binary 
------
Binary data read/write is supported. 

Raw 
----
Raw read/write is supported. DALiuGE will read/write the data using standard IO read/write methods. 

Other supported encodings 
##########################

Path
----
Path encoding does actually encode data; rather, it is a way to indicate that the data to be used by the consumer AppDROP is the `path` of the Data, rather than the data itself. This is necessary when using PyFuncApps or BashShellApps, where the control over data read/write is potentially removed from DALiuGE as a side-effect. 