Application development
=======================

This section describes what developers need to do
to write a new class that can be used
as an Application Drop in |daliuge|.

.. default-domain:: py

Class
-----

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
------------------------------

When writing an application, developers should add specific custom
`Doxygen <https://www.doxygen.nl/>`_ comments to the source code.
These comments describe the application and can
be used to automatically generate a DALiuGE component so that the
application can be used in the *EAGLE* Logical Graph Editor.

The comments should be contained within a *EAGLE_START* and *EAGLE_END*
pair.

The *category* param is should be set to *DynlibApp* for C/C++ code,
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
a continuous integration step should process the source code.
The processing will:

* combine the Doxygen output XML into a single XML file
* transform the XML into an EAGLE palette file
* push the palette file to the *ICRAR/EAGLE_test_repo* repository.
