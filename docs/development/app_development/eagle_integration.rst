.. _eagle_integration:

Component Description Generation
================================
In order to present graph developers with well defined components for their workflow development, EAGLE uses descriptions of the components based on a JSON schema. Typically a number of these component descriptions are saved and used together in a so-called *palette*. The |daliuge| system provides two ways to create such palettes. One internal to EAGLE and another one by using special `Doxygen <https://www.doxygen.nl/>`_ markup inline with the component code. The latter method allows the component developer to keep everything required to describe a component in a single place, together with the code itself. The manual one allows graph developers to define and use components, which are otherwise not available, like for example bash components.

Automatic EAGLE Palette Generation
----------------------------------
The automatic generation of a *palette* involves three steps:

#. Markup of code using custom Doxygen comments
#. Running of xml2palette.py, which is a small python script that uses the Doxygen documentation comments to generate a EAGLE palette with the required JSON format.
#. (optional) commit the resulting palette file to a graph repository.

The last two steps can be integrated into a CI build system and would then be executed automatically with any commit of the component source code. Very often one directory of source code contains multiple source files, each of which contain multiple components. The resulting palette will include descriptions of all the components found in a directory.

Generate palette using xml2palette.py
"""""""""""""""""""""""""""""""""""""

The xml2palette.py script is located in the tools directory within the DALiuGE repository. It is designed to generate a single palette file for a input directory containing doscumented code. The script has the following dependencies:

#. Doxygen
#. xsltproc

The xml2palette.py script can be run using this command line:

.. code-block:: none

  python3 xml2palette.py -i <path_to_input_directory> -o <path_output_file>


The xml2palette.py script expects several enviroment variables to be present:

#. PROJECT_NAME (e.g. "leap")
#. PROJECT_VERSION (e.g. "abcd1234")
#. GIT_REPO (e.g. "https://gitlab.com/ska-telescope/icrar-leap-accelerate.git")

These requirements can all be combined together on a single command line:

.. code-block:: none

  PROJECT_NAME=<project_name> PROJECT_VERSION=$(git rev-parse --short HEAD) GIT_REPO=$(git config --get remote.origin.url) python3 xml2palette.py -i <path_to_input_directory> -o <path_output_file>

Component Doxygen Markup Guide
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In order to support the direct usage of newly written application components in the EAGLE editor, the |daliuge| system supports a custom set of Doxygen directives and tools. When writing an application component, developers can add specific custom `Doxygen <https://www.doxygen.nl/>`_ comments to the source code. These comments describe the component and can be used to automatically generate a JSON DALiuGE component description which in turn can be used in the *EAGLE*. A few basic rules to remember:

#. The |daliuge| specific comments should be contained within a *EAGLE_START* and *EAGLE_END* pair.

#. The *category* param should be set to *DynlibApp* for C/C++ code, and *PythonApp* for Python code.

The additional comments describe both the input/output ports for a component, and the parameters of a component. Shown below are example comments for C/C++ and Python applications.

Parameters
""""""""""

Component Parameters are specified using the "param" command from doxygen. The command is followed by the name of the parameter, followed by a description. We encode multiple pieces of information within the name and description. The name must begin with "param/". This is used to disambiguate from ports, described later. The "param/" prefix will be removed during processing and only the remainder of the name will appear in the component. Names may not contain spaces. The description contains five pieces of information, separated by '/' characters: a user-facing name, the default value, the parameter type, an access descriptor (readonly/readwrite), and a "precious" flag. Note that the first line of the description must end with a '/' character.

.. code-block:: python

  # @param param/<internal_name> <user-facing name>/<default_value>/<type>/<access_descriptor>/<precious>/<description>
  #
  # e.g.
  #
  # @param param/start_frequency Start Frequency/500/Integer/readwrite/False/
  #     \~English the start frequency to read from
  #     \~Chinese 要读取的起始频率

The **precious** flag indicates that the value of the parameter should always be shown to the user, even when the parameter contains its default value. The flag also enforces that the parameter will always end-up on the command line, regardless of whether it contains the default value.

Ports
"""""

Component ports are (somewhat confusingly) also specified using the "param" from doxygen. However in this case the following text must begin with "port/". The port name and data type follow the "port/" prefix, separated by '/' characters.

.. code-block:: python

  # @param[<direction>] port/<internal_name> <user-facing name>/<type>/<description>
  #
  # e.g.
  #
  # @param[in] port/config Config/String/
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
  * \param[in] param/start_frequency Start Frequency/500/Integer/readwrite/False/
  *     \~English the start frequency to read from
  *     \~Chinese 要读取的起始频率
  * \param[in] param/end_frequency End Frequency/500/Integer/readwrite/False/
  *     \~English the end frequency to read from
  *     \~Chinese 要读取的结束频率
  * \param[in] param/channels Channels/64/Integer/readonly/False/
  *     \~English how many channels to load
  *     \~Chinese 需要加载的通道数量
  * \param[in] port/config Config/String/
  *     \~English the configuration of the input_port
  *     \~Chinese 输入端口的设置
  * \param[in] port/event Event/Event/
  *     \~English the event of the input_port
  *     \~Chinese 输入端口的事件
  * \param[out] port/File File/File/
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
  # @param[in] param/start_frequency Start Frequency/500/Integer/readwrite/False/
  #     \~English the start frequency to read from
  #     \~Chinese 要读取的起始频率
  # @param[in] param/end_frequency End Frequency/500/Integer/readwrite/False/
  #     \~English the end frequency to read from
  #     \~Chinese 要读取的结束频率
  # @param[in] param/channels Channels/64/Integer/readonly/False/
  #     \~English how many channels to load
  #     \~Chinese 需要加载的通道数量
  # @param[in] port/config Config/String/
  #     \~English the configuration of the input_port
  #     \~Chinese 输入端口的设置
  # @param[in] port/event Event/Event/
  #     \~English the event of the input_port
  #     \~Chinese 输入端口的事件
  # @param[out] port/File File/File/
  #     \~English the file of the output_port
  #     \~Chinese 输出端口的文件
  # @par EAGLE_END


Manual EAGLE Palette Generation
-------------------------------
The *palette* and *logical graph* JSON formats are almost interchangable. The two formats differ only by filename extension and by a single attribute in the JSON contents (modelData.fileType is "graph" versus "palette"). In fact one can save a graph as a palette. Defining a component in EAGLE requires the activation of the *palette mode*. More details can be found in the `EAGLE <https://eagle-dlg.readthedocs.io/en/latest/palettes.html>`_ documentation.
