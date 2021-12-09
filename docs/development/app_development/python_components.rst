.. default-domain:: py

.. _python_components:

Python Components
=================
We strongly recommend to use the `component development template <https://github.com/ICRAR/daliuge-component-template>`_ we are providing, please refer to chapter :doc:`../template_primer` for more details. The following is based on the usage of the template.

Change to the sub-directory ``my_components`` and open the file ``app_components.py``:

.. _graphs.figs.tmpl_app:
.. figure:: ../../images/tmpl_app_components.png


You will need to modify quite a bit of this file in order to produce a complete template. However, we've tried to make this as easy as possible. The file has three main parts:

#. Generic module level in-line documentation
#. Import section: This will bind the component to the |daliuge| system.
#. Doxygen/Sphinx formatted component documentation: This will be used to generate JSON files compatible with EAGLE and will thus enable people to use your components in the visual graph editor.
#. The actual functionality of a standard Python component is contained in the class MyAppDROP. That in turn inherits from the |daliuge| class BarrierAppDROP.

This base class defines all methods and attributes that derived class need to function correctly. This new class will need a single method called ``run <dlg.drop.InputFiredAppDROP.run>``,that receives no arguments (except ``self``), and executes the logic of the application.

Basic development method
------------------------
Since the code already implements a sort of a Hello World example we will simply modify that a bit to see how the development would typically work. In the spirit of test driven development, we will first change the associated test slightly and then adjust the component code accordingly to make the tests pass again. First let's see whether the existing tests pass:

.. _graphs.figs.tmpl_test:
.. figure:: ../../images/tmpl_pytest.png

All good! Now change to the tests directory and load the file ``test_components.py``:

.. _graphs.figs.tmpl_test_py:
.. figure:: ../../images/tmpl_test_py.png

and replace the string ``MyAppDROP`` with ``MyFirstAppDROP`` everywhere. Save the file and execute the test again.\:

.. _graphs.figs.tmpl_test_py_error:
.. figure:: ../../images/tmpl_test_py_error.png

Alright, that looks pretty serious (as expected)! It actually states that it failed in the file ``__init__.py``, thus let's fix this by replacing ``MyAppDROP`` with ``MyFirstAppDROP`` there and run pytest again:

.. _graphs.figs.tmpl_test_py_error2:
.. figure:: ../../images/tmpl_test_py_error2.png

Oops, that still fails! This time in the actual `appComponents.py`` file. Let's do the same replace in that file and run pytest again:

.. _graphs.figs.tmpl_test_py_fixed:
.. figure:: ../../images/tmpl_test_py_fixed.png

GREAT! In exactly the same manner you can work along to change the functionality of your component and always keep the tests up-to-date.

Obviously you can add more than one component class to the file ``app_components.py``, or add multiple files to the directory. Just don't forget to update the file ``__init__.py`` accordingly as well.

Remove boylerplate and add your documentation
---------------------------------------------
Next step is to clean up the mess from the boylerplate template and update the documentation of our new |daliuge| component. The first thing is to remove the files `ABOUT_THIS_TEMPLATE.md` and `CONTRIBUTING.md`. The next step is to update the file `README.md`. Open that file and remove everything above ``<!--  DELETE THE LINES ABOVE THIS AND WRITE YOUR PROJECT README BELOW -->`` and then do exactly what is written on that line: *Write your project README below!*. Then save the file. Make sure the LICENSE file contains a license you (and your employer) are happy with. If you had to install any additional Python packages, make sure they are listed in the ``requriements-test.txt`` and ``requirements.txt`` files and modify the file ``setup.py`` as required. Finally add more detailed documentation to the docs directory. This will then also be published on readthedocs whenever you push to the main branch. After that you will have a pretty nice and presentable component package already.

Using parameters
----------------
TODO!

Adding input and output ports
-----------------------------
TODO!

