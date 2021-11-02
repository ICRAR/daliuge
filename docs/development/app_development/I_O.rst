I/O
===

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
