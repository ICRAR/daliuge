.. _eagle_integration:

Automatic EAGLE Component Description Generation
------------------------------------------------

In order to support the direct usage of newly written application components in the EAGLE editor, the |daliuge| system includes a custom set of Doxygen directives and tools. When writing an application component, developers can add specific custom
`Doxygen <https://www.doxygen.nl/>`_ comments to the source code.
These comments describe the application and can
be used to automatically generate a DALiuGE component so that the
application can be used in the *EAGLE* Logical Graph Editor.

The comments should be contained within a *EAGLE_START* and *EAGLE_END*
pair.

The *category* param should be set to *DynlibApp* for C/C++ code,
and *PythonApp* for Python code.

These comments describe both the input/output ports for a component,
and the parameters of a component. Shown below are example comments
for C/C++ and Python applications.

C/C++

.. code-block:: c

  /*!
  * \brief Load a CASA Measurement Set in the DaliugeApplication Framework
  * \details We will build on the LoadParset structure - but use the contents
  * of the parset to load a measurement set.
  * \par EAGLE_START
  * \param gitrepo $(GIT_REPO)
  * \param version $(PROJECT_VERSION)
  * \param category DynlibApp
  * \param[in] param/start_frequency/500/Integer
  *     \~English the start frequency to read from\n
  *     \~Chinese 要读取的起始频率\n
  *     \~
  * \param[in] param/end_frequency/500/Integer
  *     \~English the end frequency to read from\n
  *     \~Chinese 要读取的结束频率\n
  *     \~
  * \param[in] param/channels/64/Integer
  *     \~English how many channels to load\n
  *     \~Chinese 需要加载的通道数量\n
  *     \~
  * \param[in] port/config
  *     \~English the configuration of the input_port\n
  *     \~Chinese 输入端口的设置\n
  *     \~
  * \param[in] port/event
  *     \~English the event of the input_port\n
  *     \~Chinese 输入端口的事件\n
  *     \~
  * \param[out] port/File
  *     \~English the file of the output_port \n
  *     \~Chinese 输出端口的文件\n
  *     \~
  * \par EAGLE_END
  */

Python

.. code-block:: python

  ##
  # @brief Load a CASA Measurement Set in the DaliugeApplication Framework
  # @details We will build on the LoadParset structure - but use the contents
  # of the parset to load a measurement set.
  # @par EAGLE_START
  # @param gitrepo $(GIT_REPO)
  # @param version $(PROJECT_VERSION)
  # @param category PythonApp
  # @param[in] param/start_frequency/500/Integer
  #     \~English the start frequency to read from\n
  #     \~Chinese 要读取的起始频率\n
  #     \~
  # @param[in] param/end_frequency/500/Integer
  #     \~English the end frequency to read from\n
  #     \~Chinese 要读取的结束频率\n
  #     \~
  # @param[in] param/channels/64/Integer
  #     \~English how many channels to load\n
  #     \~Chinese 需要加载的通道数量\n
  #     \~
  # @param[in] port/config
  #     \~English the configuration of the input_port\n
  #     \~Chinese 输入端口的设置\n
  #     \~
  # @param[in] port/event
  #     \~English the event of the input_port\n
  #     \~Chinese 输入端口的事件\n
  #     \~
  # @param[out] port/File
  #     \~English the file of the output_port \n
  #     \~Chinese 输出端口的文件\n
  #     \~
  # @par EAGLE_END


Once the comments are added to the source code and pushed to a repository
a continuous integration step can then use the tools provided by the |daliuge| system to process the source code and produce the component descriptions readable by EAGLE.

The processing will:

* combine the Doxygen output XML into a single XML file
* transform the XML into an EAGLE palette file
* push the palette file to the *ICRAR/EAGLE_test_repo* repository.
