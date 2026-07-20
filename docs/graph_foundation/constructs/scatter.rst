.. _scatter:

Scatter / Gather
-----------------

The Scatter is a construct that duplicates and distributes the components encapsulated inside, and Gather is a construct that collects the distributed results of a Scatter. It is analagous to an MPI_Scatter/Gather sequence. 

Scatter constructs require an InputApp that performs the splitting of input data. Currently, this is supported by the GenericScatterApp, and is limited to any input data that can be represented as a ``numpy`` array. 

