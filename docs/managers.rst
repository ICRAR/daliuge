
.. _drop.managers:

DROP Managers
-------------

The runtime environment of |daliuge| consists on a hierarchy of *DROP Managers*.
DROP Managers offer a standard interface to external entities to interact with
the runtime system, allowing users to submit physical graphs, deploy them, let
them run and query their status.

DROP Managers are organized hierarchically, mirroring the topology of the
environment hosting them, and thus enabling scalable solutions. The current design is flexible
enough to add more intermediate levels if necessary in the future. The
hierarchy levels currently present are:

* A *Node DROP Manager* exists for every node in the system.
* Nodes are grouped into *Data Islands*, and thus a *Data Island DROP Manager*
  exists at the Data Island level.
* On top of the Data Islands is the *Master DROP Manager*.

Sessions
^^^^^^^^

The DROP Managers' work is to manage and execute physical graphs. Because
more than one physical graph can potentially be deployed in the system, DROP
Managers introduce the concept of a *Session*. Sessions represent a physical graph
execution, which are completely isolated from one another. This has two main
consequences:

* Submitting the same physical graph to a DROP Manager will create two different
  sessions
* Two physical graph executions can run at the same time in a given DROP
  Manager.

Sessions have a simple lifecycle: they are first created, then a physical graph
is attached into them (optionally by parts, or all in one go), after which the
graph can be deployed (i.e., the DROPs are created). This leaves the session in
a running state until the graph has finished its execution, at which point the
session is finished and can be deleted.


.. _node_drop_manager:

Node DROP Manager
^^^^^^^^^^^^^^^^^

*Node DROP Managers* sit at the bottom of the DROP management hierarchy. They
are the direct responsible for creating and deleting DROPs, and for ultimately
running the system.

The Node DROP Manager works mainly as a collection of sessions that are created,
populated and run. Whenever a graph is received, it checks that it's valid
before accepting it, but delays the creation of the DROPs until deployment time.
Once the DROPs are created, the Node DROP Manager exposes them via Pyro to allow
remote method executions on them.

Data Island DROP Manager
^^^^^^^^^^^^^^^^^^^^^^^^

*Data Island DROP Managers* sit on top of the Node DROP Managers. They follow the
assumed topology where a set of nodes is grouped into a logical *Data Island*.
The Data Island DROP Manager is the public interface of the whole Data Island to
external users, relaying messages to the individual Node DROP Managers as
needed.

When receiving a physical graph, the Data Island DROP Manager will first check
that the nodes of the graph contain all the necessary information to route them
to the correct Node DROP Managers. At deployment time it will also make sure that
the inter-node DROP relationships (which are invisible from the Node DROP
Managers' point of view) are satisfied by obtaining DROP Pyro proxies and
linking them correspondingly.

Master DROP Manager
^^^^^^^^^^^^^^^^^^^

The Master DROP Manager works exactly like the Data Island DROP Manager but one
level above. At this level a set of Data Islands are gathered together to form a
single group of which the Master DROP Manager is the public interface.


Interface
^^^^^^^^^

All managers in the hierarchy expose a REST interface to external users. The
interface is exactly the same independent of the level of the manager in the
hierarchy.

The hierarchy contains the following entry points::

 GET    /api
 POST   /api/sessions
 GET    /api/sessions
 GET    /api/sessions/<sessionId>
 DELETE /api/sessions/<sessionId>
 GET    /api/sessions/<sessionId>/status
 POST   /api/sessions/<sessionId>/deploy
 GET    /api/sessions/<sessionId>/graph
 GET    /api/sessions/<sessionId>/graph/status
 POST   /api/sessions/<sessionId>/graph/append

The interface indicate the object with which one is currently interacting, which
should be self-explanatory. ``GET`` methods are queries performed on the
corresponding object. ``POST`` methods send data to a manager to create new
objects or to perform an action. ``DELETE`` methods delete objects from the
manager.

Of particular attention is the ``POST /api/sessions/<sessionId>/graph/append``
method used to feed a manager with a physical graph. The content of such request
is a JSON list of objects, where each object contains a full description of a
DROP to be created by the manager.


Clients
^^^^^^^

Python clients are available to ease the communication with the different
managers. Apart from that, any third-party tool that talks the HTTP
protocol can easily interact with any of the managers.
