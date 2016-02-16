DROPs
-----

DROPs are at the center of the DFMS. DROPs are representations of data and
applications, making them manageable by DFMS.

Lifecycle
^^^^^^^^^

The lifecycle of a DROP is simple and follows the basic principle of writing
once, read many times. Additionally, it also allows for data deletion.

A DROP starts in the **INITIALIZED** state, meaning that its data is not
present yet. From there it jumps into **COMPLETED** once its data has been
written, optionally passing through **WRITING** if the writing occurs
*through* dfms (see `Input/Output`_). Once in the **COMPLETED** state the data
can be read as many times as needed. Eventually, the DROP will transition to
**EXPIRED**, denying any further reads. Finally the data is deleted and the DROP
moves to the final **DELETED** state. If any I/O error occurs the DROP will be
moved to the **ERROR** state.

Events
^^^^^^

Changes in a DROP state, and other actions performed on a DROP, will fire named
events which are sent to all the interested subscribers. Users can subscribe to
particular named events, or to all events.

Relationships
^^^^^^^^^^^^^

DROPs are connected between them and create a graph representing an execution
plan, where inputs and outputs are connected to applications, establishing the
following possible relationships:

* A data DROP is the *input* of an application DROP; on the other hand
  the application is a *consumer* of the data DROP.
* Likewise, a data DROP can be a *streaming input* of an application
  DROP in which case the application is seen as a *streaming consumer* from
  the data DROP's point of view.
* Finally, a data DROP can be the *output* of an application DROP, in
  which case the application is the *producer* of the data DROP.

The difference between *normal* inputs/consumers and their *streaming*
counterpart is their granularity. In the normal case, inputs only notify their
consumers when they have reached the **COMPLETED** state. Streaming inputs on
the other hand notify consumers each time data is written into them, and thus
allow for a continuous operation of applications as data gets written into
their inputs. Once all the data has been written, the normal event notifying
that the DROP has moved to the **COMPLETED** state is also fired.

Execution
^^^^^^^^^

A collection of interconnected DROPs has the ability to advance its own
execution. This is internally implemented the DROP event mechanism as follows:

* Once a data DROP moves to the COMPLETED or ERROR state it will fire an event
  to all its consumers. Consumers will then deem if they can start their
  execution depending on their nature and configuration. A specific type of
  application is the *BarrierAppDROP*, which waits until all its inputs are in
  the **COMPLETED** to start its execution.
* On the other hand, data DROPs receive an even every time their producers
  finish their execution. Once all the producers of a DROP have finished, the
  DROP moves itself to the **COMPLETED** state, notifying its consumers, and so
  on.

Input/Output
^^^^^^^^^^^^

I/O can be performed on the data that is represented by a DROP by obtaining 
a reference to its I/O object and calling the necessary POSIX like methods.
In this instance, the data is passing through the DROP instance. The application
is free to bypass the DROP interface and perform I/O directly on the data. 
In this case, the application must ensure that the data being written, for example,
is being placed in the correct location and is in the expected format for storage or
subsequent upstream processing by other application DROPs. The DFMS provides various 
commonly used data DROPs with their associated I/O objects.


Drop Channels
^^^^^^^^^^^^^

DROPs that a connected by an edge in a physical graph that are deployed on separate nodes or islands from each other are automatically given a Pyro stub (remote method invocation interface) in order for them to communicate to each other. It's the job of the Master DROP and Island Managers to generate and exchange stubs between DROP instances before the graph is deployed to the various data islands and nodes within islands respectively. If there is no DROP separation within a physical graph parition then its implied that the DROPs are going to be executed within a single address space, as a result, basic method calls are used.


Drop Component Interface
^^^^^^^^^^^^^^^^^^^^^^^^

The DFMS framework uses Docker containers as its primary interface to 3rd party applications. Docker containers have the following benefits over traditional tools management:

* Portability.
* Versioning and component reuse.
* Lightweight footprint.
* Simple maintenance.

The application programmer can make use to the `dfms.apps.dockerapp <http://dfms.readthedocs.org/en/latest/api/apps.html?highlight=docker#module-dfms.apps.dockerapp>`_ object as an interface to their container and the DROP framework. Refer to the documentation for details. 
