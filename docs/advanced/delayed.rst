.. _delayed:

Delayed Execution
=================

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
