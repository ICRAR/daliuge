.. _drops:

Drops
-----

Drops are at the center of the |daliuge|. Drops are representations of data and applications, making them manageable by |daliuge|.

Lifecycle
^^^^^^^^^

The lifecycle of a Drop is simple and follows the basic principle of writing once, read many times. Additionally, it also allows for data deletion.

A Drop starts in the **INITIALIZED** state, meaning that its data is not present yet. From there it jumps into **COMPLETED** once its data has been written, optionally passing through **WRITING** if the writing occurs *through* |daliuge| (see `Input/Output`_). Once in the **COMPLETED** state the data can be read as many times as needed. Eventually, the Drop will transition to **EXPIRED**, denying any further reads. Finally the data is deleted and the Drop moves to the final **DELETED** state. If any I/O error occurs the Drop will be moved to the **ERROR** state.

.. _drop.events:

Events
^^^^^^

Changes in a Drop state, and other actions performed on a Drop, will fire named events which are sent to all the interested subscribers. Users can subscribe to particular named events, or to all events.

In particular the :ref:`node_drop_manager` subscribes to all events generated by the Drops it manages. By doing so it can monitor all their activities and perform any appropriate action as required. The Node Drop Manager, or any other entity, can thus become a Graph Event Manager, in the sense that they can subscribe to all events sent by all Drops and make use of them.

.. _drop.relationships:

Relationships
^^^^^^^^^^^^^

Drops are connected and create a dependency graph representing an execution plan, where inputs and outputs are connected to applications, establishing the following possible relationships:

#. None or many data Drop(s) can be the *input* of an application Drop; and the application is the *consumer* of the data Drop(s).
#. A data Drop can be a *streaming input* of an application Drop in which case the application is seen as a *streaming consumer* from the data Drop's point of view.
#. None or many Drop(s) can be the *output* of an application Drop, in which case the application is the *producer* of the data Drop(s).
#. An application is never a consumer or producer of another application;  conversely a data Drop never produces or consumes another data Drop. 

The difference between *normal* inputs/consumers and their *streaming* counterpart is their granularity. In the normal case, inputs only notify their consumers when they have reached the **COMPLETED** state, after which the consumers can open the Drop and read their data. Streaming inputs on the other hand notify consumers each time data is written into them (alongside with the data itself), and thus allow for a continuous operation of applications as data gets written into their inputs. Once all the data has been written, the normal event notifying that the Drop has moved to the **COMPLETED** state is also fired. 

.. _drop.io:

Input/Output
^^^^^^^^^^^^

I/O can be performed on the data that is represented by a Drop by obtaining a reference to its I/O object and calling the necessary POSIX-like methods.  In this case, the data is passing through the Drop instance. The application is free to bypass the Drop interface and perform I/O directly on the data, in which case it uses the data Drop ``dataURL`` to find out the data location.  It is the responsibility of the application to ensure that the I/O is occurring in the correct location and using the expected format for storage or subsequent upstream processing by other application Drops.

|daliuge| provides various commonly used data components with their associated I/O storage classes, including in-memory, file-base, S3, and `NGAS <https://ngas.readthedocs.io/>`_ storages. It is also possible to access the contant of a plain URL and use that as a data source.

When using and developing a |daliuge| workflow the details of the I/O mechanisms are completely hidden, but users just need to be aware of the differences and limitations of using either of them. Memory and Files or remote data objects are just not really the same in terms of I/O capabilities and performance. The most important difference is between memory and all the other methods, since plain memory really only works for Python and dynamic library based components. A bash component for example simply does not know how to deal with some memory block handed over to it. That is why EAGLE does prevent such connections between components in the first place.

When developing *application* components most of these details are also transparent, as long as the application component is using the provided POSIX-like access mechanisms. It is possible though to bypass those inside a component and perform all I/O independently of the framework. Even on that level there are still two ways, one is to use the provided data url from the framework, but not use the I/O methods. The even more extreme way is to just open some named file or channel without |daliuge| knowing anything about it. This latter way is strongly discouraged, since it will create unpredictable side-effects, which are almost impossible to identify in a large distributed environment. 


.. _drop.channels:

Drop Channels
^^^^^^^^^^^^^

During a |daliuge| workflow execution one application drop produces the data of a data drop, which in turn is consumed by another application drop. That means that data drops are essentially providing the data transfer methods between applications. The |daliuge| translator tries to minimise data movement and thus in many cases no transfer is actually happening, but the data drop transfers to COMPLETED state once it has received all data and passes that event on to the consumer application(s). The consumer applications in turn will use the provided read method to access the data directly.

In cases when data drops are accessed from separate nodes or islands the managers automatically produce a drop proxy on the remote nodes providing a remote method invocation (RMI) interface to allow the producers or consumers to execute the required I/O methods. It's the job of the Master Drop and Island Managers to generate and exchange these proxies and connect them to the correct Drop instances when the graph is deployed to potentially multiple data islands and nodes. If there is no Drop separation within a physical graph partition then its implied that the Drops are going to be executed within a single address space, and, as a result, basic method calls are used between Drop instances.

In addition to the hand-over of the handle to the consumer once the data drop is COMPLETED |daliuge| also supports streaming data directly from one application drop to another during run-time. Like for most streaming applications this is based on the completion of a block of bytes transferred, thus the intermediate data drop still has a meaning and could in priciple be any standard data drop. In practice the only viable solutions are memory based drops, like plain memory, and shared memory.


.. _drop.component.iface:

Drop Component Interface
^^^^^^^^^^^^^^^^^^^^^^^^

The |daliuge| framework uses Docker containers as its primary interface to 3rd party applications. Docker containers have the following benefits over traditional tools management:

#. Portability.
#. Versioning and component reuse.
#. Lightweight footprint.
#. Simple maintenance.

The application programmer can make use of the :ref:`DockerApp <api.dlg.apps.dockerapp>` which is the interface between a Docker container and the Drop framework. Refer to the documentation for details.

Other applications not based on Docker containers can be written as well. Any application must derive at least from ``AppDrop``, but an easier-to-use base class is the ``BarrierAppDrop``, which simply requires a ``run`` method to be written by the developer (see :ref:`api.dlg.Drop` for details). |daliuge| ships with a set of pre-existing applications to perform common operations, like a TCP socket listener and a bash command executor, among others. See :ref:`api.dlg.apps` for more examples. In addition we have developed a stand-alone tool (`dlg_paletteGen <https://icrar.github.io/dlg_paletteGen/>`_), which enables the automatic generation of |daliuge| compatible component descriptions from existing code. In this way it is possible to enable to usage of big existing public or propietary libraries of algorithms, like e.g. `Astropy <https://astropy.org>`_ within the |daliuge| eco-system.
