.. _eagle_integration:

Component Description Generation
================================
In order to present graph developers with well defined components for their workflow development, EAGLE uses descriptions of the components based on a JSON schema. Typically a number of these component descriptions are saved and used together in a so-called *palette*. The |daliuge| system provides two ways to create such palettes. One internal to EAGLE and another one by using special `Doxygen <https://www.doxygen.nl/>`_ markup inline with the component code. The latter method allows the component developer to keep everything required to describe a component in a single place, together with the code itself. The manual one allows graph developers to define and use components, which are otherwise not available, like for example bash components.

Manual EAGLE Palette Generation
-------------------------------
The *palette* and *logical graph* JSON formats are almost interchangable. The two formats differ only by filename extension and by a single attribute in the JSON contents (modelData.fileType is "graph" versus "palette"). In fact one can save a graph as a palette. Defining a component in EAGLE requires the activation of the *palette mode*. More details can be found in the `EAGLE <https://eagle-dlg.readthedocs.io/en/latest/palettes.html>`_ documentation.

Automatic EAGLE Palette Generation
----------------------------------
The automatic generation of a *palette* involves four steps:

* Markup of code
* Running of doxygen using a provided config file
* Running of xml2palette.py, which is a small tool to convert the XML files generated in the step above into the required JSON format.
* (optional) commit the resulting palette file to a graph repository.

The last three steps can be integrated into a CI build system and would then be executed automatically with any commit of the component source code. Very often one directory of source code contains multiple source files, each of which contain multiple components. The resulting palette will include descriptions of all the components found in a directory.

Component Doxygen Markup Guide
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In order to support the direct usage of newly written application components in the EAGLE editor, the |daliuge| system supports a custom set of Doxygen directives and tools. When writing an application component, developers can add specific custom `Doxygen <https://www.doxygen.nl/>`_ comments to the source code. These comments describe the component and can be used to automatically generate a JSON DALiuGE component description which in turn can be used in the *EAGLE*.

The comments should be contained within a *EAGLE_START* and *EAGLE_END* pair.

The *category* param should be set to *DynlibApp* for C/C++ code, and *PythonApp* for Python code.

These comments describe both the input/output ports for a component, and the parameters of a component. Shown below are example comments for C/C++ and Python applications.

Parameters
""""""""""

Component Parameters are specified using the "param" command from doxygen. The command is followed by the name of the parameter, followed by a description. We encode multiple pieces of information within the name and description. The name must begin with "param/". This is used to disambiguate from ports, described later. The "param/" prefix will be removed during processing and only the remainder of the name will appear in the component. Names may not contain spaces. The description contains four pieces of information, separated by '/' characters: a user-facing name, the default value, the parameter type, and an access descriptor (readonly/readwrite). Note that the first line of the description must end with a '/' character.

.. code-block:: python

  # @param param/<internal_name> <user-facing name>/<default_value>/<type>/<access_descriptor>/
  #
  # e.g.
  #
  # @param param/start_frequency Start Frequency/500/Integer/readwrite/
  #     \~English the start frequency to read from
  #     \~Chinese 要读取的起始频率

Ports
"""""

Component ports are (somewhat confusingly) also specified using the "param" from doxygen. However in this case the following text must begin with "port/". The port name and data type follow the "port/" prefix, separated by '/' characters.

.. code-block:: python

  # @param[<direction>] port/<name>/<type>
  #
  # e.g.
  #
  # @param[in] port/config/String
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
  * \param gitrepo $(GIT_REPO)
  * \param version $(PROJECT_VERSION)
  * \param category DynlibApp
  * \param[in] param/start_frequency Start Frequency/500/Integer/readwrite/
  *     \~English the start frequency to read from
  *     \~Chinese 要读取的起始频率
  * \param[in] param/end_frequency End Frequency/500/Integer/readwrite/
  *     \~English the end frequency to read from
  *     \~Chinese 要读取的结束频率
  * \param[in] param/channels Channels/64/Integer/readonly/
  *     \~English how many channels to load
  *     \~Chinese 需要加载的通道数量
  * \param[in] port/config/String
  *     \~English the configuration of the input_port
  *     \~Chinese 输入端口的设置
  * \param[in] port/event/Event
  *     \~English the event of the input_port
  *     \~Chinese 输入端口的事件
  * \param[out] port/File/File
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
  # @param gitrepo $(GIT_REPO)
  # @param version $(PROJECT_VERSION)
  # @param category PythonApp
  # @param[in] param/start_frequency Start Frequency/500/Integer/readwrite/
  #     \~English the start frequency to read from\n
  #     \~Chinese 要读取的起始频率\n
  #     \~
  # @param[in] param/end_frequency End Frequency/500/Integer/readwrite/
  #     \~English the end frequency to read from\n
  #     \~Chinese 要读取的结束频率\n
  #     \~
  # @param[in] param/channels Channels/64/Integer/readonly/
  #     \~English how many channels to load\n
  #     \~Chinese 需要加载的通道数量\n
  #     \~
  # @param[in] port/config/String
  #     \~English the configuration of the input_port\n
  #     \~Chinese 输入端口的设置\n
  #     \~
  # @param[in] port/event/Event
  #     \~English the event of the input_port\n
  #     \~Chinese 输入端口的事件\n
  #     \~
  # @param[out] port/File/File
  #     \~English the file of the output_port \n
  #     \~Chinese 输出端口的文件\n
  #     \~
  # @par EAGLE_END


Once the comments are added to the source code and pushed to a repository a continuous integration step can then use the tools provided by the |daliuge| system to process the source code and produce the component descriptions readable by EAGLE.

The processing will:

* combine the Doxygen output XML into a single XML file
* transform the XML into an EAGLE palette file (JSON)
* push the palette file to a GitHub/GitLab repository (optional).
