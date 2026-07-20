.. _global:

Global
######

Global variables that are accessible across the graph may be added using the GlobalVariables construct. This is a construct that allows you to define an arbitrary number of parameters that may be accessed throughout the graph.

Use cases for the GlobalVariables construct include:

    - Constant path names; for example, data-output directories (e.g. ``/scratch/pawseyXXX``)
    - Global iteration numbers; for example, to keep Loop iterations consistent throughout the graph.

.. note::
    The GlobalVariable construct is replaced after translation. This means it cannot set values at runtime.

