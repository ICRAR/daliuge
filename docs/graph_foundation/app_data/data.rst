.. _data_patterns:

Data patterns
#############

Memory DROPs
=============
MemoryDROPs can have values hard-coded into them, or have input/output pass through them without any user-specified data. Some simple rules of thumb to follow are: 

- Ensure port connections are established through IO, not PyData

Further information can be found in :ref:`ports`. 
  
Path-based DROPs
================
Path-based drops are DROPs that refer to data on the filesystem. DALiuGE supports the following Path-based DROPS: 

- FileDROPS: These map to files on the system, to which data may be read from or written to at Runtime. 
- DirectoryDROPS: These map to directories on the system and may be read from or created by DALiuGE. Note that DALiuGE currently only supports making the directory itself, or tracking it as an :ref:`side effect`

Path-expansion 
---------------

.. _side effect:

App side-effect
----------------
A lot of applications and python functions that perform data transformation produce output files themselves, which means the data is written outside of the DALiuGE engine. This is a *side-effect*; data is created somewhere outside the control of DALiuGE.

A classic example of this is a python function that requires the name of the output file as input. In DALiuGE, the dataflow model requires the file to be registered as an output, but we require the information as input. 

DALiuGE supports tracking side effects for Path-based DROPs; so long as the output uses a Path encoding, DALiuGE will have awareness of the data and be able to track it within the rest of the graph. It will even be able to use the DALiuGE-created filename of the DROP, reducing the need to add filenaming logic to the graph.   