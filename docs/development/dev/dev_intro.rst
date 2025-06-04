.. _dev_intro:

Introduction to Component development
=====================================

What are *Components*?
----------------------

Nomenclature
~~~~~~~~~~~~
The following chapters and sections will use some terms in a specific meaning. The definitions are given below.

#. Component: *Component* refers to a |daliuge| compliant implementation of some functionality used in the execution of a workflow. A component consists of the |daliuge| interface wrapper and the code implementing the desired functionality. In some cases the actual functional code is not integrated with the interface, but just executed by the interface. There are three main types of components:

   * Application Component: |daliuge| interface wrapper and application code.
   * Data Component: |daliuge| interface wrapper around an I/O channel. Examples are standard files, memory, and S3.
   * Service Component: A *Service Component* is a special component, providing access to services like a database or some other client/server system used within a workflow.

#. Construct: A *Construct* is a complex *Component*, which may contain other *Components*.
#. Node: *Graph Node* or *Palette Node* refers to a JSON representation of a *Component* in a |daliuge| graph or palette.
#. Drop: *Drop* is a |daliuge| specific term used to describe instances of data, application or service components at execution time. In general developers don't have to dive into the Drop level.

In practice the component interface wrapper code is written in Python. |daliuge| provides generic wrappers and base classes to make the development of components more straight forward and hide most of the |daliuge| specifics. In some cases the generic wrappers can be used directly to develop functioning Graph and Palette Nodes using EAGLE, without writing any code. Examples are bash nodes (:doc:`app_development/bash_components`).

Seperation of concerns
~~~~~~~~~~~~~~~~~~~~~~
The |daliuge| system has been designed with the separation of concerns in mind. People or groups of people can work independently on the development of 

#. the logic of a workflow (graph), 
#.  the detailed definition of a Node, and a collection of nodes (Palette)
#. the interface Component code and 
#. the actual functional implementation of the required algorithm of a component.
  
In fact it is possible to create logical workflows and run them, without having any substantial functional code at all. On the opposite side it is also possible to develop the functional software without considering |daliuge| at all. This feature also allows developers to write wrapper components around existing software without the need to change that package. Whatever can be called on a \*NIX bash command line, in a docker container, or can be loaded as a python function can also run as part of a |daliuge| workflow. 

The Component code has been designed to be as non-intrusive as possible, while still allowing for highly optimized integration of code, down to the memory level. With a bit of care developers can fairly easily also run and test each of these layers independently. 

Integration of Layers
~~~~~~~~~~~~~~~~~~~~~
The |daliuge| system consists of three main parts:

#. EAGLE for graph development
#. Translator to translate logical graphs to physical graphs and partition and statically schedule them.
#. Execution Engine to execute physical graphs on small or very large compute clusters.

All three of them are essentially independent and can be used without the others running. The EAGLE visual graph editor is a web application, which deals with *Nodes*, i.e. with JSON descriptions of *Components*. It also allows users to define these descriptions starting from node templates. Users can group and store sets of defined nodes as *palettes*. The nodes of one or multiple palettes are then used to construct a workflow graph. Graphs and Palettes are stored as JSON files and they are essentially the same, except for the additional arrangement and visual information in a graph file. A graph can be stored as a palette and then used to create other graphs.

In addition to constructing the nodes manually in EAGLE it is also possible to generate the JSON description of the nodes in a palette from the component interface code by means of special in-line doxygen documentation tags. This procedure is described in detail in :doc:`app_development/eagle_app_integration`. The idea here is that the developers of the component can stay within their normal development environment and just provide some additional in-line code documentation to allow people to use their new component in their workflows. 

Component development
---------------------
In particular if people already have existing Python code or large libraries, we are now recommending to first try our new stand-alone tool `dlg_paletteGen <https://icrar.github.io/dlg_paletteGen/>`_), which enables the automatic generation of |daliuge| compatible component descriptions from existing code. The tool parses the code and generates a palette containing the classes, class methods and functions found in the code. It does not require any code changes at all. However, for best usability of the resulting components good docstrings and Python type hinting is highly recommended. The tool supports three standard docstring formats: Google, Numpy and ReST. Also for new components and in particular for all these small little functions, which will be required to perform exactly the tasks you need, we now recommend to simply put them in a .py file and let the tool do the rest. Please refer to the tool documentation for more information. 

Simple bash and python application components as well as data components can be developed completely inside EAGLE and, since EAGLE interfaces with gitHub and gitLab, it even provides some kind of development workflow. However, for more complex and serious component development it is strongly recommended to use the `component development template <https://github.com/ICRAR/daliuge-component-template>`_ we are providing. The template covers application and data components and provides everything to get you started, including project setup, testing, format compliance, build, documentation and release, continuous integration and more. Although it is meant to be used to develop a whole set of components, it is quite useful even for just a single one. We are still actively developing the template itself and thus a few things are still missing:

#. Installation into the |daliuge| runtime is possible, but not the way we would like to have it.
#. Automatic palette generation is not yet integrated.

Please note that most of the Components Developers Guide is based on using the template.