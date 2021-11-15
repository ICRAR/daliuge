* :ref:`genindex`
* :ref:`search`

.. _app_index:

|daliuge| Application Component Developers Guide
################################################

This chapter describes what developers need to do
to write a new application component that can be used
as an Application Drop during the execution of a |daliuge| graph.

Detailed instructions can be found in the respective sections for
each type of components. There are also separate sections describing
integration and testing during component development. As mentioned already, for more complex and serious component development we strongly recommend to use the `component development template <https://github.com/ICRAR/daliuge-component-template>`_ we are providing, please refer to chapter :doc:`../template_primer` for more details. Most of the following sub-sections of this documentation are based on the usage of the template.

*NOTE: The DCDG is work in progress!*

.. toctree::
 :maxdepth: 2

 bash_components
 python_components
 python_function_components
 dynlib_components
 docker_components
 service_components
 I_O
 wrap_existing
 test_and_debug
 eagle_integration
 deployment_testing
