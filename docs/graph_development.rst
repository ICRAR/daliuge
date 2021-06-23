Graph development
=================

.. default-domain:: py

This section describes the different ways
users can develop graphs (either Logical or Physical)
to work with |daliuge|.

As explained in :doc:`graphs`,
|daliuge| describes computations
in terms of Directed Acyclic Graphs.
Two different types of graphs are used
throughout application development:
*Logical Graphs*, a high-level, compact representation
of the application logic,
and *Physical Graphs*, a detailed description
of each individual processing step.
When submitting a graph for execution,
users submit |pgs| to the runtime component of |daliuge|.
Therefore a |lg| needs to be first translated into a |pg|
before submitting it for execution.
The individual steps that occur during this translation process
are detailed in :ref:`graphs.translation`.

Given all the above,
the following graph development techniques are available
for users to creates graphs and submit them for execution:

 * :ref:`Use the Logical Graph Editor <graph_dev.lge>`
   to create a |lg|, which can then be translated into a |pg|.
 * Manually, or automatically, :ref:`create a Physical Graph from scratch <graph_dev.pg>`.
 * :ref:`Use the delayed function <graph_dev.delayed>` to generate a |pg|.


.. _graph_dev.lge:

Using the Logical Graph Editor
------------------------------

The :doc:`examples` section
has some examples on how to use the Logical Graph Editor
to compose Logical Graphs.

Please be aware that this section is old and incomplete,
and also refers to the "old" Logical Graph Editor,
which although hasn't been removed, will soon be deprecated.
A new Logical Graph Editor called *EAGLE*
is currently in the works.
This new editor implements more features,
is more complete in its support,
and is visually easier to follow.
More information about it will come soon.

.. _graph_dev.pg:

Directly creating a Physical Graph
----------------------------------

In some cases using the Logical Graph Editor is not possible.
This can happen because its (currently) limited capabilities,
or because somehow some information would be lost
during the translation process.

In these cases, producing the Physical Graph directly
is still possible.
Once a Physical Graph is produced,
it can be partitioned and mapped
(see the steps in :ref:`graphs.translation`)
either via the |daliuge| utilities,
or by the user directly.
Finally, the Physical Graph can be sent
to one of the Drop Managers
for execution.

.. _graph_dev.delayed:

Using :func:`dlg.delayed`
-------------------------

|daliuge| ships with a `Dask <https://dask.org/>`__ emulation layer
that allows users write code
like they would for using under Dask,
but that executes under |daliuge| instead.
In Dask users write normal python code
to represent their computation.
This code is not executed immediately though;
instead its execution is *delayed*
by wrapping the function calls
with the ``delayed`` `Dask function <https://docs.dask.org/en/latest/delayed.html>`__,
until a final ``compute`` call is invoked,
at which point the computation is submitted to the Dask runtime agents.
These agents execute the computation logic
and return a result to the user.

To emulate Dask,
|daliuge| also offers a ``delayed`` function
(under ``dlg.delayed``)
that allows users to write normal python code.
The usage pattern is exactly the same
as that of Dask:
users wrap their function calls with the ``delayed`` function,
and end up calling the ``compute`` method
to be obtain the final result.

Under the hood,
|daliuge| returns intermediate placeholder objects
on each invocation to ``delayed``.
When ``compute`` is invoked,
these objects are used to compute a Physical Graph,
which is then submitted to one of the Drop Managers
for execution.
|daliuge| doesn't have the concept
of returning the final result back to the user.
In order to imitate this,
a final application is appended automatically
to the Physical Graph before submission.
This final application allows the ``compute`` function
to connect to it.
Once this final application receives the final result
of the Physical Graph
it then sends it to the ``compute`` function,
who presents the result to the user.

.. |lg| replace:: logical graph
.. |lgs| replace:: logical graphs
.. |pg| replace:: physical graph
.. |pgs| replace:: physical graphs
