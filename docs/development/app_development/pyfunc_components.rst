.. _pyfunc_components:

Pyfunc Components
=================

Pyfunc components are generalized python component that can be configured to behave as a custom python component entirely through component parameters and application arguments. A pyfunc component
maps directly to an existing python function or a lambda expression, named application arguments and input ports are mapped to the function keyword args, and the result is mapping to the output port.

Port Parsers
------------

Pyfunc components when interfacing with data drops may utilize one of several builtin port parsing formats.

* Pickle
* Eval
* Path
* Url
* Npy

Basic setup
-----------

Note
----

Only a single parser and 