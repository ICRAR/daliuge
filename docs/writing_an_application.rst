Application Component Development
=================================

This section describes what developers need to do
to write a new application component that can be used
as an Application Drop during the execution of a |daliuge| graph.

Types of Application Components
===============================

|daliuge| supports four main types of Application Components.

* Python application components
* Dynamic Library application components
* Docker application components
* Bash shell application components

as an extension to Dynamic Library components |daliuge| supports application components using MPI internally. In future we are planning to also support Singularity containers.

The first two application component types offer the highest integration with the framework itself, since they provide the most efficient way of communication and data transfer between components. Bash components only allow the usage of intermediate files. On the other hand using bash components is by far the easiest way to get things up and running. Essentially anything that can be called on the bash command line can also be used as an application component in |daliuge|.

Since |daliuge| itself is written in Python, Python is also the first language of choice when it comes to writing application components. The Dynamic Library application components make use of the extensibility of Python to call compiled dynamic library functions directly. Docker components are running docker containers within the execution framework and are the preferred way to isolate runtime environments from the framework. |daliuge| does support the usage of Apache Plasma as a shared memory store between application components, including docker components.

This wide range of supported types of application components provides a very high potential of re-useability of existing software, while still offering the ability to tightly integrate and optimize newly developed components.

.. default-domain:: py

Python Application Component Class
----------------------------------

Developers need to write a new python class
that derives from the :class:`dlg.drop.BarrierAppDROP` class.
This base class defines all methods and attributes
that derived class need to function correctly.
This new class will need a single method
called :attr:`run <dlg.drop.InputFiredAppDROP.run>`,
that receives no arguments,
and executes the logic of the application.

I/O
---

An application's input and output drops
are accessed through its
:class:`inputs <dlg.drop.AppDROP.inputs>` and
:attr:`outputs <dlg.drop.AppDROP.outputs>` members.
Both of these are lists of :class:`drops <dlg.drop.AbstractDROP>`,
and will be sorted in the same order
in which inputs and outputs
were defined in the Logical Graph.
Each element can also be queried
for its :attr:`uid <dlg.drop.AbstractDROP.uid>`.

Data can be read from input drops,
and written in output drops.
To read data from an input drop,
one calls first the drop's
:attr:`open <dlg.drop.AbstractDROP.open>` method,
which returns a descriptor to the opened drop.
Using this descriptor one can perform successive calls to
:attr:`read <dlg.drop.AbstractDROP.read>`,
which will return the data stored in the drop.
Finally, the drop's
:attr:`close <dlg.drop.AbstractDROP.close>` method
should be called
to ensure that all internal resources are freed.

Writing data into an output drop is similar but simpler.
Application authors need only call one or more times the
:attr:`write <dlg.drop.AbstractDROP.write>` method
with the data that needs to be written.

Automatic EAGLE Component Generation
------------------------------------

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
