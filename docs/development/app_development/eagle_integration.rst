.. _eagle_integration:

EAGLE Palette Generation
------------------------

Overview
^^^^^^^^
EAGLE is working with component descriptions based on a JSON schema for the palettes and the graphs (see `EAGLE documentation <https://eagle-dlg.readthedocs.io>`_ for more details on graphs and palettes). EAGLE does support the generation of such a description through the UI. However, this is a quite tedious and error prone process when trying to create many such descriptions. Obviously the person who knows best what a component is all about and which parameters are required to run it, is the developer of the component itself. Also, when a component is further developed, the JSON signature might need to be changed as well and then having to remember to update these JSON descriptions through EAGLE is just not very practical. Thus we have developed a method to allow Python and C/C++ component developers to add special EAGLE related doxygen tags in-line into the code. We have also generated a doxygen configuration file extracting those entries into xml files. In addition there is also a tool to parse the XML and convert it into the JSON required by EAGLE. As described below, this can be run manually, but it is also possible to integrate it into a continuous integration workflow like e.g. Travis to trigger it automatically when changes are committed to the repository. The following paragraphs describe the manual and the automatic usage and there is also a section detailing the markup requirements for both Python and C/C++.

Manual Usage
^^^^^^^^^^^^
TODO

Integration with Continuous Integration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Once the comments are added to the source code and pushed to a repository
a continuous integration step can then use the tools provided by the |daliuge| system to process the source code and produce the component descriptions readable by EAGLE.

The processing will:

* combine the Doxygen output XML into a single XML file
* transform the XML into an EAGLE palette file (JSON)
* push the palette file to a GitHub/GitLab repository (optional).

TODO: The above is not complete and needs updates.

Doxygen markup
^^^^^^^^^^^^^^

In order to support the direct usage of newly written application components in the EAGLE editor, the |daliuge| system supports a custom set of Doxygen directives and tools. When writing an application component, developers can add specific custom
`Doxygen <https://www.doxygen.nl/>`_ comments to the source code.
These comments describe the application and can
be used to automatically generate a JSON DALiuGE component description
which can be used in the *EAGLE* Logical Graph Editor.

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
  * \param[in] param/start_frequency/500/Integer/readwrite
  *     \~English the start frequency to read from\n
  *     \~Chinese 要读取的起始频率\n
  *     \~
  * \param[in] param/end_frequency/500/Integer/readwrite
  *     \~English the end frequency to read from\n
  *     \~Chinese 要读取的结束频率\n
  *     \~
  * \param[in] param/channels/64/Integer/readonly
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
  # @param[in] param/start_frequency/500/Integer/readwrite
  #     \~English the start frequency to read from\n
  #     \~Chinese 要读取的起始频率\n
  #     \~
  # @param[in] param/end_frequency/500/Integer/readwrite
  #     \~English the end frequency to read from\n
  #     \~Chinese 要读取的结束频率\n
  #     \~
  # @param[in] param/channels/64/Integer/readonly
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

