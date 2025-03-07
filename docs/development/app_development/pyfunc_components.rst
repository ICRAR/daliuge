.. _pyfunc_components:

Pyfunc Components
=================

Pyfunc components are generalized python components that can be configured to behave as a custom python component entirely through component parameters and application arguments. A pyfunc component
maps directly to an existing python function or a lambda expression, named application arguments and input ports are mapped to the function keyword args, and the result is mapping to the output port.

Port Parsers
------------

Pyfunc components when interfacing with data drops may use one of several builtin port parsing formats.

- Binary
- Pickle
- Dill 
- Numpy 
- Base64
- UTF-8

Port parsing is now possible on a per-port basis, which means that a PyFunc application may take into account input parameters from various data sources of different origins and encodings.  