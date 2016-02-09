Graphs
------

In order to perform some execution in the system, it must first be represented
in the form of a graph. Graphs contain both tasks and data, and are separated 

Physical graph
^^^^^^^^^^^^^^

A *physical graph* is a collection of inter-connected DROPs representing an
execution plan. The nodes of a physical graph are DROPs representing either
data or applications.

Edges on the graph always run between a data DROP and an application DROP. This
establishes a set of reciprocal relationships between DROPs:

* A data DROP is the *input* of an application DROP; on the other hand
  the application is a *consumer* of the data DROP.
* Likewise, a data DROP can be a *streaming input* of an application
  DROP (see :doc:`drops` in which case the application is seen
  as a *streaming consumer* from the data DROP's point of view.
* Finally, a data DROP can be the *output* of an application DROP, in
  which case the application is the *producer* of the data DROP.

Physical graphs are the final product fed into the :doc:`managers`.
