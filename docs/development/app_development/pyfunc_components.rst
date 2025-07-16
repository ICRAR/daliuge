.. _pyfunc_components:

PyFuncApp Components
=========================

PyFuncApp are the most used components in the system and rarely written from scratch, but based on existing code that is parsed by `dlg_paletteGen <https://icrar.github.io/dlg_paletteGen/>`_. PyFuncApp components are wrapping existing code into a custom DALiuGEApp component entirely through component parameters and application arguments. The wrapping is performed by *dlg_paletteGen* and does not require any special knowledge by the code developer. *dlg_paletteGen* can parse completely un-modified Python modules from any source, including C-extensions and PyBind11 based modules and generate so-called *PyFuncApp* components from the functions, classes and methods in those modules. Using this method gives the |daliuge| users access to the vast ecosphere of Python modules, including numpy, scipy, astropy, to name just a few. A PyFuncApp component maps named |daliuge| ApplicationArguments and input ports to the function's positional and keyword args, and the function's returned result is mapped to the output port.

In-line Python code
-------------------
PyFuncApp components can also be used to run in-line Python function code. This is done by setting the *func_name* parameter to the name of a function defined in the *func_code* component parameter to a string containing the Python function code. The code will be parsed and executed like any normal PyFuncApp component. This is useful for defining small pieces of code that do not require a full module. For example a simple function that adds two numbers can be defined as follows:

.. code-block:: python

    def add(a:int=1, b:int=1) -> int:
        """
        Function to add two numbers
        """
        return a + b


In this case the *func_name* parameter would be set to *add* and the *func_code* parameter would contain the code above. The two ApplicationArguments *a* and *b* would need to be added as well. The function can then be used like any other PyFuncApp component.


Port Parsers
------------

PyFuncApp components when interfacing with data drops may use one of several builtin port parsing formats, the default is dill, which provides the most robust way to serialize and deserialize Python objects across multiple compute nodes in a cluster. The following port parsers are available:

- dill
- pickle 
- npy 
- path
- utf-8
- eval
- dataurl
- binary
- raw

Port parsing is now possible on a per-port basis, which means that a PyFuncApp application may take into account input parameters from various data sources of different origins and encodings. The *path* encoding is special in that it will not parse the data, but rather pass the *path* to the data as a string. This is useful for components that require a filename as input an read/wwrite the data to disk. The *raw* encoding is also special in that it will not parse the data, but rather pass the raw bytes of the data as a string. This is useful for binary data that needs to be passed to a function that can read it directly. The *binary* encoding is similar to *raw*, but will parse the data into a binary format that can be passed to a function.
The *utf-8* encoding is used for text data that needs to be passed to a function. The *dataurl* encoding is used for data that is encoded in a data URL format, which is a base64 encoded string that can be passed to a function. The *eval* encoding is used for data that needs to be evaluated as Python code, which is useful for passing Python expressions. The *npy* encoding is used for data that is encoded in a numpy array format, which is useful for passing numpy data arrays to a function. The *pickle* and *dill* encodings are used for data that is encoded in the pickle or dill format, which is useful for serializing and passing Python objects. Note that the *pickle* and *dill* encodings may fail to serialize and deserialize complex objects, but *dill* is more robust and thus the default.
