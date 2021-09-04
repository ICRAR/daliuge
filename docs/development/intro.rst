.. _intro:

What are *Components*?
======================

Nomenclature
------------

The following chapters and sections will use some terms in a specific meaning. The definitions are given below.

#. Component: *Component* refers in particular to the |daliuge| wrapper implementation of some functionality used in the execution of a workflow. Although the actual functionality might be contained in the wrapper directly, in general this is not required.
    * Application Component: |daliuge| wrapper around some code.
    * Data Component: |daliuge| wrapper around an I/O channel. Examples are standard files, memory, S3 and Apache Plasma.
    * Service Component: A *Service Component* is a special component, like for instance a database or some other client/server system used within a workflow.
#. Construct: A *Construct* is a complex *Component*, which may contain other *Components*.
#. Node: *Graph Node* or *Palette Node* refers to a JSON representation of a *Component* in a |daliuge| graph or palette.
#. Drop: *Drop* is a |daliuge| specific term used to describe instances of data, application or service components at execution time.

According to the definition above a component essentially is some wrapper code. In practice this wrapper code is written in Python. We do provide generic wrappers and base classes to make the development of components more straight forward and hide most of the |daliuge| specifics. In some cases the generic wrappers can be used directly to develop functioning Graph and Palette Nodes using EAGLE, without writing any code. Examples are bash and Python function nodes (:doc:`bash_components` and :doc:`python_function_components`).
