
.. _drop.managers:

Drop Managers
-------------

The runtime environment of |daliuge| consists on a hierarchy of *Drop Managers*.
Drop Managers offer a standard interface to external entities to interact with
the runtime system, allowing users to submit physical graphs, deploy them, let
them run and query their status.

Drop Managers are organized hierarchically, mirroring the topology of the
environment hosting them, and thus enabling scalable solutions. The current design is flexible
enough to add more intermediate levels if necessary in the future. The
hierarchy levels currently present are:

* A *Node Drop Manager* exists for every node in the system.
* Nodes are grouped into *Data Islands*, and thus a *Data Island Drop Manager*
  exists at the Data Island level.
* On top of the Data Islands is the *Master Drop Manager*.

Sessions
^^^^^^^^

The Drop Managers' work is to manage and execute physical graphs. Because
more than one physical graph can potentially be deployed in the system, Drop
Managers introduce the concept of a *Session*. Sessions represent a physical graph
execution, which are completely isolated from one another. This has two main
consequences:

* Submitting the same physical graph to a Drop Manager will create two different
  sessions
* Two physical graph executions can run at the same time in a given Drop
  Manager.

Sessions have a simple lifecycle: they are first created, then a physical graph
is attached into them (optionally by parts, or all in one go), after which the
graph can be deployed (i.e., the Drops are created). This leaves the session in
a running state until the graph has finished its execution, at which point the
session is finished and can be deleted.


.. _node_drop_manager:

Node Drop Manager
^^^^^^^^^^^^^^^^^

*Node Drop Managers* sit at the bottom of the Drop management hierarchy. They
are the direct responsible for creating and deleting Drops, and for ultimately
running the system.

The Node Drop Manager works mainly as a collection of sessions that are created,
populated and run. Whenever a graph is received, it checks that it's valid
before accepting it, but delays the creation of the Drops until deployment time.
Once the Drops are created, the Node Drop Manager exposes them via Pyro to allow
remote method executions on them.

Data Island Drop Manager
^^^^^^^^^^^^^^^^^^^^^^^^

*Data Island Drop Managers* sit on top of the Node Drop Managers. They follow the
assumed topology where a set of nodes is grouped into a logical *Data Island*.
The Data Island Drop Manager is the public interface of the whole Data Island to
external users, relaying messages to the individual Node Drop Managers as
needed.

When receiving a physical graph, the Data Island Drop Manager will first check
that the nodes of the graph contain all the necessary information to route them
to the correct Node Drop Managers. At deployment time it will also make sure that
the inter-node Drop relationships (which are invisible from the Node Drop
Managers' point of view) are satisfied by obtaining Drop Pyro proxies and
linking them correspondingly.

Master Drop Manager
^^^^^^^^^^^^^^^^^^^

The Master Drop Manager works exactly like the Data Island Drop Manager but one
level above. At this level a set of Data Islands are gathered together to form a
single group of which the Master Drop Manager is the public interface.


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
Drop to be created by the manager.


Clients
^^^^^^^

Python clients are available to ease the communication with the different
managers. Apart from that, any third-party tool that talks the HTTP
protocol can easily interact with any of the managers.
