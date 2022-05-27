.. _pyfunc_components:

Pyfunc Components
=================

Pyfunc components are generalized python components that can be configured to behave as a custom python component entirely through component parameters and application arguments. A pyfunc component
maps directly to an existing python function or a lambda expression, named application arguments and input ports are mapped to the function keyword args, and the result is mapping to the output port.

Port Parsers
------------

Pyfunc components when interfacing with data drops may utilize one of several builtin port parsing formats.

* Pickle - Reads and writes data to pickle format
* Eval - Reads data using eval() function and writes using repr() function
* Npy - Reads and writes to .npy format
* Path - Reads the drop path rather than data
* Url - Reads the drop url rather than data


Note
""""

Only a single port parser can currently be used for all input ports of a Pyfunc. This is subject to change in future.
