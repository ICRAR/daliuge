.. _eagle_app_integration:

Component Description (Palette) Generation
==========================================
In order to present graph developers with well defined components for their workflow development, EAGLE uses descriptions of the components based on a JSON schema. Typically a number of these component descriptions are saved and used together in a so-called *palette*. The |daliuge| system provides two ways to create such palettes, one is by using EAGLE (but that is pretty cumbersome) and another one using a stand-alone tool to automatically generate palettes from installed Python modules or source code. This command line tool is called *dlg_paletteGen* and can be installed using pip. The documentation is available `here <https://icrar.github.io/dlg_paletteGen/>`_. *dlg_paletteGen* can parse completely un-modified Python modules from any source, including C-extensions and PyBind11 based modules and generate so-called *PyFuncApp* components from the functions, classes and methods in those modules. It can also parse custom written |daliuge| component code and generate so-called *DALiuGEApp* component palettes. Creating *PyFuncApp* palettes is by far the most common use case and covers almost all situations.

*NOTE: The following section is for special use cases only!*

Component Doxygen Markup Guide
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*dlg_paletteGen* provides a way to parse propietary `Doxygen <https://www.doxygen.nl/>`_ markup inline with |daliuge| specific component code. This allows the |daliuge| component developer to keep everything required to describe a component in a single place, together with the code itself. The manual one allows graph developers to define and use components, which are otherwise not available, like for example bash components.

In order to support the direct usage of newly written application components in the EAGLE editor, the |daliuge| system supports a custom set of Doxygen directives and tools. When writing an application component, developers can add specific custom `Doxygen <https://www.doxygen.nl/>`_ comments to the source code. These comments describe the component and can be used to automatically generate a JSON DALiuGE component description which in turn can be used in the *EAGLE*. A few basic rules to remember:

#. The |daliuge| specific comments should be contained within a *EAGLE_START* and *EAGLE_END* pair.

#. The *category* param should be set to *DynlibApp* for C/C++ code, and *PythonApp* for Python code.

#. The *construct* param should be set to Scatter or Gather. Or omitted entirely for components that will not be embedded inside a construct.

The additional comments describe both the input/output ports for a component, and the parameters of a component. Shown below are example comments for C/C++ and Python applications.

Construct
"""""""""

If a component is intended to implement a scatter or gather construct, then the *construct* param should be added to the Doxygen markup. When a component is flagged with a construct param, the component will be added to the palette as usual, but the component will also be added to the palette a second time embedded within an appropriate construct. Here is an example usage:

.. code-block:: python

  # @param construct Scatter

Parameters
""""""""""

Component Parameters are specified using the "param" command from doxygen. The command is followed by the name of the parameter, followed by a description. We encode multiple pieces of information within the name and description. The name must begin with "param/". This is used to disambiguate from ports, described later. The "param/" prefix will be removed during processing and only the remainder of the name will appear in the component. Names may not contain spaces. The description contains five pieces of information, separated by '/' characters: a user-facing name, the default value, the parameter type, an access descriptor (readonly/readwrite), and a "precious" flag. Note that the first line of the description must end with a '/' character.

.. code-block:: python

  # @param <internal_name> <user-facing name>/<default_value>/<type>/<field_type>/<access_descriptor>/<options>/<precious>/<positional>/<description>
  #
  # e.g.
  #
  # @param start_frequency Start Frequency/500/Integer/ComponentParameter/readwrite//False/False/
  #     \~English the start frequency to read from
  #     \~Chinese 要读取的起始频率

The **precious** flag indicates that the value of the parameter should always be shown to the user, even when the parameter contains its default value. The flag also enforces that the parameter will always end-up on the command line, regardless of whether it contains the default value.

The **positional** flag indicates that this parameter is a positional argument on a command line, and will be added to the command line without a prefix.

Component Parameters vs. Application Arguments
""""""""""""""""""""""""""""""""""""""""""""""

There are two different types of parameter that can be specified on a component. These two types are: Component Parameter and Application Argument. Component parameters are intended to direct the behaviour of the DALiuGE component itself, while Application arguments are intended to direct the application underneath the component. For example, a component may have Component Parameter describing the number of CPUs to be used for execution, but a application argument for the arguments on the command line for the component.

The two types of parameters use different keywords (ComponentParameter vs. ApplicationArgument), as shown in the example below.

.. code-block:: python

  # @param start_frequency Start Frequency/500/Integer/ComponentParameter/readwrite//False/False/
  #     \~English the start frequency to read from
  * @param method Method/mean/Select/ApplicationArgument/readwrite/mean,median/False/False/
  *     \~English The method used for averaging


Parameter Types
"""""""""""""""

Available types are:

#. String
#. Integer
#. Float
#. Boolean
#. Select
#. Password
#. Json
#. Python
#. Object

The Select parameters describe parameters that only have a small number of valid values. The valid values are specified in the "options" part of the Doxygen command, using a comma separated list. For example:

.. code-block:: python

  * @param method Method/mean/Select/ApplicationArgument/readwrite/mean,median/False/False/
  *     \~English The method used for averaging

All other parameter types have empty options.

Ports
"""""

Component ports are (somewhat confusingly) also specified using the "param" from doxygen. However, field types of InputPort and OutputPort are used.

.. code-block:: python

  # @param <internal_name> <user-facing name>/<default_value>/<type>/<field_type>/<access_descriptor>/<options>/<precious>/<positional>/<description>
  #
  # e.g.
  #
  # @param config Config//String/InputPort/readwrite//False/False/
  #     \~English the configuration of the input_port
  #     \~Chinese 输入端口的设置

Complete example for C/C++
""""""""""""""""""""""""""

.. code-block:: c

  /*!
  * \brief Load a CASA Measurement Set in the DaliugeApplication Framework
  * \details We will build on the LoadParset structure - but use the contents
  * of the parset to load a measurement set.
  * \par EAGLE_START
  * \param category DynlibApp
  * \param start_frequency Start Frequency/500/Integer/ComponentParameter/readwrite//False/False/
  *     \~English the start frequency to read from
  *     \~Chinese 要读取的起始频率
  * \param end_frequency End Frequency/500/Integer/ComponentParameter/readwrite//False/False/
  *     \~English the end frequency to read from
  *     \~Chinese 要读取的结束频率
  * \param channels Channels/64/Integer/ApplicationArgument/readonly//False/False/
  *     \~English how many channels to load
  *     \~Chinese 需要加载的通道数量
  * \param method Method/mean/Select/ApplicationArgument/readwrite/mean,median/False/False/
  *     \~English The method used for averaging
  * \param config Config//String/InputPort/readwrite//False/False/
  *     \~English the configuration of the input_port
  *     \~Chinese 输入端口的设置
  * \param event Event//Event/InputPort/readwrite//False/False/
  *     \~English the event of the input_port
  *     \~Chinese 输入端口的事件
  * \param file File//File/OutputPort/readwrite//False/False/
  *     \~English the file of the output_port
  *     \~Chinese 输出端口的文件
  * \par EAGLE_END
  */

Complete example for Python
"""""""""""""""""""""""""""

.. code-block:: python

  ##
  # @brief Load a CASA Measurement Set in the DaliugeApplication Framework
  # @details We will build on the LoadParset structure - but use the contents
  # of the parset to load a measurement set.
  # @par EAGLE_START
  # @param category PythonApp
  # @param start_frequency Start Frequency/500/Integer/ComponentParameter/readwrite//False/False/
  #     \~English the start frequency to read from
  #     \~Chinese 要读取的起始频率
  # @param end_frequency End Frequency/500/Integer/ComponentParameter/readwrite//False/False/
  #     \~English the end frequency to read from
  #     \~Chinese 要读取的结束频率
  # @param channels Channels/64/Integer/ApplicationArgument/readonly//False/False/
  #     \~English how many channels to load
  #     \~Chinese 需要加载的通道数量
  # @param method Method/mean/Select/ApplicationArgument/readwrite/mean,median/False/False/
  #     \~English The method used for averaging
  # @param config Config//String/InputPort/readwrite//False/False/
  #     \~English the configuration of the input_port
  #     \~Chinese 输入端口的设置
  # @param event Event//Event/InputPort/readwrite//False/False/
  #     \~English the event of the input_port
  #     \~Chinese 输入端口的事件
  # @param file File//File/OutputPort/readwrite//False/False/
  #     \~English the file of the output_port
  #     \~Chinese 输出端口的文件
  # @par EAGLE_END


Manual EAGLE Palette Generation
-------------------------------
The *palette* and *logical graph* JSON formats are almost interchangable. The two formats differ only by filename extension and by a single attribute in the JSON contents (modelData.fileType is "graph" versus "palette"). In fact one can save a graph as a palette. Defining a component in EAGLE requires the activation of the *palette mode*. More details can be found in the `EAGLE <https://eagle-dlg.readthedocs.io/en/latest/palettes.html>`_ documentation.
