.. default-domain:: py

.. _python_components:

Python Components
=================

Developers need to write a new python class
that derives from the :class:`dlg.drop.BarrierAppDROP` class.
This base class defines all methods and attributes
that derived class need to function correctly.
This new class will need a single method
called :attr:`run <dlg.drop.InputFiredAppDROP.run>`,
that receives no arguments,
and executes the logic of the application.
