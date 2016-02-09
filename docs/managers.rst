DROP Managers
=============

The runtime environment of DFMS consists on a hierarchy of *DROP Managers*.
DROP Managers offer a standard interface to external entities to interact with
the runtime system, allowing users to submit physical graphs, deploy them, let
them run and query their status.

DROP Managers are hierarchically organized, mirroring the topology of their
environment, and thus enable scalable solutions. The current design is flexible
enough to add more intermediate levels if necessary in the future. The
hierarchy levels currently present are:

* A *Node DROP Manager* (or **NDM** from now on) exists for every node in the
  system. NIMs sit at the bottom of the hierarchy, and therefore they are
  responsible for creating and deleting DROPs, and for ultimately running the
  system.
* Nodes are grouped into *Data Islands*, and thus a *Data Island DROP Manager*
  (**DIDM** from now on) exists at the Data Island level.
* On top of the DIDMs is the *Master DROP Manager* (**MDM** from now on).


Interface
---------

All managers in the hierarchy expose a REST interface to external users. The interface is exactly the same independent of the level of the manager in the hierarchy.

The hierarchy contains the following entry points::

 POST   /api/sessions
 GET    /api/sessions
 GET    /api/sessions/<sessionId>
 DELETE /api/sessions/<sessionId>
 GET    /api/sessions/<sessionId>/status
 POST   /api/sessions/<sessionId>/deploy
 GET    /api/sessions/<sessionId>/graph
 GET    /api/sessions/<sessionId>/graph/status
 POST   /api/sessions/<sessionId>/graph/append


Python clients are available to ease the communication with the different managers.
