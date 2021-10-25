Graph development
=================

.. default-domain:: py

This section describes the different ways
users can develop workflows (either Logical or Physical)
to work with |daliuge|.

As explained in :doc:`architecture/graphs`,
|daliuge| describes computations
in terms of Directed Graphs.
Two different classes of graphs are used
in the |daliuge| workflow development:

#. *Logical Graphs*, a high-level, compact representation of the application logic. Logical Graphs are directed graphs, but not acyclic.
#. *Physical Graphs*, a detailed description of each individual processing step. Physical Graphs are Directed Acyclic Graphs (DAG)

When submitting a graph for execution,
the |daliuge| engine expects a |pg|. Therefore a |lg| needs to be first translated into a |pg| before submitting it for execution.
The individual steps that occur during this translation process are detailed in :ref:`graphs.translation`.

The following graph development techniques are available for users to creates graphs and submit them for execution:

#. :ref:`Use the Logical Graph Editor EAGLE <graph_dev.lge>`
   to create a |lg|, which can then be translated into a |pg|.
#. Manually, or automatically, :ref:`create a Physical Graph from scratch <graph_dev.pg>`.
#. :ref:`Use the delayed function <graph_dev.delayed>` to generate a |pg|.


.. _graph_dev.lge:

Using the Logical Graph Editor EAGLE
------------------------------------

Please refer to the `EAGLE documentation <https://eagle-dlg.readthedocs.io>`__ for detailed information. When using the EAGLE graph editor the translator and engine levels are not really exposed to the user, thus in the following we will describe a few examples of how to directly generate a graph.

.. _graph_dev.pg:

Directly creating a Physical Graph
----------------------------------

In some cases using EAGLE is not possible or not the preferred way of working.

In these cases, developing a Physical Graph directly is still possible. One example where this approach is used, are the |daliuge| engine tests, more specifically in the subdirectory ``daliuge-engine/test/apps``. As can be seen there the graph is constructed directly in Python, by using high-level class methods to instantiate application and data nodes and then adding inputs and outputs, or producers and consumers, for applications and data nodes, respectively. Note that you only have to add either a *consumer* to a data node, or the equivalent *input* to the application node. Once the whole graph is constructed in that way, it can be executed directly using using the utility method ``droputils.DROPWaiterCtx``. For smaller graphs this is a perfectly valid approach, but it is quite tedious when it comes to larger scale graphs.

The test ``daliuge-engine/test/manager/test_scalability`` contains an example of how to generate a big graph using higher level functions. However, this approach is only feasible for large but low complexity graphs. Since the constructs (e.g. Scatter, Gather, Loop) are also exposed as classes, they can also be used in the same way as normal apps to construct more complex graphs.

Simple Hello World graph
^^^^^^^^^^^^^^^^^^^^^^^^
.. highlight:: ipython
   :linenothreshold: 5

Like every software framework project we need to describe a Hello World example. This one is straight from the |daliuge| test in ``daliuge-engine/test/apps/test_simple.py``::

   from dlg.apps.simple import HelloWorldApp
   from dlg.drop import FileDROP

   h = HelloWorldApp('h', 'h')
   f = FileDROP('f', 'f')
   h.addOutput(f)
   f.addProducer(h)
   h.execute()

Let's look at this in detail. Lines 1 and 2 import the HelloWorldApp and the FileDROP classes, respectively, both of them are part of the |daliuge| code base. Line 4 instanciates an object from the HelloWorldApp class and assigns an object ID (oid) and a unique ID (uid) to the resulting object. In our example both of them are simply the string ``'h'``. We then instantiate the FileDROP with ``oid = uid = 'f'`` in line 5. In line 6 we add the instance of the FileDROP (f) as an output to the HelloWorldApp drop (h). In line 7 we add the HelloWorldApp drop (h) as a producer for the FileDROP (f). NOTE: It would have been sufficient to have either line 6 or line 7, but just to show the calls we do both here (and it does not break things either). Finally in line 8 we call the execute method of the HelloWorldApp (h). This will trigger the execution of the rest of the graph as well. Note that there was no need to care about any detail at all. In fact it is not even obvious whether anything happend at all when executed. In order to check that let's have a look where the file had been written to::

   in [1] print(f.path, f.size)
   /tmp/daliuge_tfiles/f 11

Means that there is a file with name f and a size of 11 bytes::

   in [2] print(len('Hello World'))
   11
   in [3] !cat $f.path
   Hello World

Seems to be what is expected! 

Parallel Hello World graph
^^^^^^^^^^^^^^^^^^^^^^^^^^

Now that was fun, but kind of boring. |daliuge| is all about paralellism, thus we'll add a bit of that::

   from dlg.apps.simple import HelloWorldApp, GenericScatterApp
   from dlg.drop import FileDROP, InMemoryDROP
   from dlg.droputils import DROPWaiterCtx
   import pickle

   m0 = InMemoryDROP('m0','m0')
   s = GenericScatterApp('s', 's')
   greets = ['World', 'Solar system', 'Galaxy', 'Universe']
   m0.write(pickle.dumps(greets))
   s.addInput(m0)
   m = []
   h = []
   f = []
   for i in range(1, len(greets)+1, 1):
      m.append(InMemoryDROP('m%d' % i, 'm%d' % i))
      h.append(HelloWorldApp('h%d' % i, 'h%d' % i))
      f.append(FileDROP('f%d' % i, 'f%d' % i))
      s.addOutput(m[-1])
      h[-1].addInput(m[-1])
      h[-1].addOutput(f[-1])
   with DROPWaiterCtx(None, f, 1):
      m0.setCompleted()


This example is a bit more busy, thus let's dissect it as well. In the import section we import a few more items, the GenericScatterApp and the InMemoryDROP as well as the pickle module. In lines 5 and 6 we instantiate an InMemoryDROP and a GenericScatterApp respectively. Line 7 just prepares the array of strings, called *greets* to be used as greeting strings. In line 7 we push that array into the memort drop *m0*. Line 8 adds *m0* to the scatter app as input. Lines 9,10 and 11 just initialize three lists and in line 12 we start a loop for the number of elements of *greets*. This loop is essentially the main construction of the rest of the graph as well as keeping all the drop objects in the three lists *m*, *h* and *f* (lines 13, 14 and 15). Each element of *greets* will be placed into a separate memory drop by the GenericScatterApp (line 16). Each of those memory drops will trigger a separate HelloWorldApp drop (line 17), which in turn will write to a separate file drop (line 18). Line 19 is using the utility *DROPWaiterCtx* method, which sets up the event subscription mechanism between the various drops in the graph. Finally in line 20 we trigger the execution by changing the status of the initial memory drop *m0* to 'COMPLETE'.

This should now have generated four output files in the default |daliuge| output directory ``/tmp/daliuge_tfiles``. If you copy and paste the above script into a file called ``parallelHelloWorld.py`` and execute it using ``ipython -i parallelHelloWorld.py`` you can check the content of the files with following commands::

   In [1]: for fd in f:
   ...:     fp = fd.path
   ...:     !cat $fp
   ...:     print()

This should produce the output::

   Hello World
   Hello Solar system
   Hello Galaxy
   Hello Universe

Note that all of the above is still limited to execution on a single node. In order to use the distributed functionality of the |daliuge| system it is still required to use the graphs, which in turn lead to the individual drops to be instantiated on the assigned compute nodes. That is also when the I/O transparency suddenly makes sense, because |daliuge| will make sure that the reads and writes are translated into remote reads and writes where and when required. Producing a distributed graph programmatically is possible, albeit a bit tedious, since it essentially requires to construct the JSON representation of the graph and then submit it to the |daliuge| island manager. This is shown in more detail in the file ``daliuge-engine/test/manager/test_scalability.py``.

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
