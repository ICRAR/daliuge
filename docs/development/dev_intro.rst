.. _dev_intro:

What are *Components*?
======================

Nomenclature
------------

The following chapters and sections will use some terms in a specific meaning. The definitions are given below.

#. Component: *Component* refers to a |daliuge| compliant implementation of some functionality used in the execution of a workflow. A component consists of the |daliuge| interface wrapper and the code implementing the desired functionality. In some cases the actual functional code is not integrated with the interface, but just executed by the interface. There are three main types of components:
    * Application Component: |daliuge| interface wrapper and application code.
    * Data Component: |daliuge| interface wrapper around an I/O channel. Examples are standard files, memory, S3 and Apache Plasma.
    * Service Component: A *Service Component* is a special component, providing access to services like a database or some other client/server system used within a workflow.
#. Construct: A *Construct* is a complex *Component*, which may contain other *Components*.
#. Node: *Graph Node* or *Palette Node* refers to a JSON representation of a *Component* in a |daliuge| graph or palette.
#. Drop: *Drop* is a |daliuge| specific term used to describe instances of data, application or service components at execution time. In general developers don't have to dive into the Drop level.

In practice the component interface wrapper code is written in Python. |daliuge| provides generic wrappers and base classes to make the development of components more straight forward and hide most of the |daliuge| specifics. In some cases the generic wrappers can be used directly to develop functioning Graph and Palette Nodes using EAGLE, without writing any code. Examples are bash and Python function nodes (:doc:`bash_components` and :doc:`python_function_components`).

Seperation of concerns
----------------------
The |daliuge| system has been designed with the separation of concerns in mind. People or groups of people can work independently on the development of 

* the logic of a workflow (graph), 
* the detailed definition of a Node, and a collection of nodes (Palette)
* the interface Component code and 
* the actual functional implementation of the required algorithm of a component. 
  
In fact it is possible to create logical workflows and run them, without having any substantial functional code at all. On the opposite side it is also possible to develop software without considering |daliuge| at all. This feature also allows developers to write wrapper components around existing software without the need to change that package. Whatever can be called on a \*NIX bash command line, in a docker container, or can be loaded as a python function can also run as part of a |daliuge| workflow. 

The Component code has been designed to be as non-intrusive as possible, while still allowing for highly optimized integration of code, down to the memory level. With a bit of care developers can fairly easily also run and test each of these layers independently. 

Integration of Layers
---------------------
The |daliuge| system consists of three main parts:

* EAGLE
* Translator
* Execution Engine

All three of them are essentially independent and can be used without the others running. The EAGLE visual graph editor is a web application, which deals with *Nodes*, i.e. with JSON descriptions of *Components*. It also allows users to define these descriptions starting from node templates, which are contained in a standard palette, called 'All Nodes', which is loaded when EAGLE is started in a browser. The nodes of one or multiple palettes are used to construct a workflow graph. Graphs and Palettes are stored as JSON files and they are essentially the same, except for the additional arrangement and visual information in a graph file. A graph can be stored as a palette and then used to create other graphs.

The JSON description of the nodes in a palette can be generated from the component interface code by means of special in-line documentation tags. This procedure is described in detail in :doc:`component_binding`.