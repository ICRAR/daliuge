.. _basics:

Hello, World
#############

Having successfully established running installation of DALiuGE applications, it is now possible to begin testing a new workflow!

To continue working on a new workflow, it is recommended to take the following steps:

*  It is *highly* recommended you complete the following interactive EAGLE tutorials in-browser:

  * `Quick Start <https://eagle.icrar.org/?tutorial=Quick%20Start>`_

  * `Graph Building <https://eagle.icrar.org/?tutorial=Graph%20Building>`_
    
* Please note that to use EAGLE to create graphs, you will need to setup a `GitHub Access Token <https://github.com/settings/tokens>`_. 

* dlg_paletteGen: This allows you to create and load Python modules as Palettes into EAGLE, and begin creating workflows directly from Python classes, methods, and functions. 
  
    * This can be installed using ``pip install dlg_paletteGen``, and then used with the ``dlg palette`` command


Basic: Using existing graph
===========================

Outcomes: 
---------

* Run an existing graph end-to-end 
* Understand the DALiuGE workspace and CLI 

Steps
------ 

To start, we want to run the following applications using the respective commands. It's recommended to do this in separate tabs in your terminal application. Ensure that for each tab, you have correctly sourced the virtual environment in which you installed DALiuGE:

**DALiuGE Translator**::
  
  dlg lgweb -t /tmp/ -d /tmp/

**Node Manager (NM)**::
  
  dlg nm -H 0.0.0.0

**Data Island Manager (DIM)**::

  dlg dim -N 0.0.0.0

  
Further information on the CLI for these is available at :ref:`cli_translator` and :ref:`cli_engine`. 
  
We will use the existing `Hello World <http://eagle.icrar.org/?service=GitHub&repository=ICRAR/dlg-reference-graphs&branch=main&path=hello_world&filename=HelloWorld-simple.graph>`_ graph to start: 

* Click on the HelloWorldApp to start with and look at the parameters 
* There should be a ``greet`` parameter, with a default value
* Modify this value to something that you would like to be; for example, "ICRAR"
* In the right-hand of the Navbar there is the "Translate" button. Click this, and you should be taken to a new webpage.
* This page shows the fully 'unrolled' phyiscal graph template produced by the translator. 
* To run the actual graph, click on the "Deploy" button in the right-hand of the navbar. This should take you to *another* webpage. 
* Cick on the "Graph" button on the webpage, and you will hopefully see a complete workflow, with both DROPs registering success in green.
  * It's also possible to review the logs on the AppDROP by clicking the 'Details' hyperlink. However, everything should have run fine so this will not produce anything of much use right now. 

Provided everything is green and has completed successfully, we can confirm the output of the graph! DALiuGE runs everything within its own directory, which by default is in you ``$HOME/dlg/`` directory. To see the output file produced by your HelloWorldApp :abbr:

* Navigate to ``$HOME/dlg/workspace`` in your file browser
* There should be a folder with the "HelloWorld-simpleX-XXXX-XX-XXTXX-XX-XX.XXXX". Enter this folder
* There will be three files in here: 
  * The output file (it will have no file extension and start with todays date.) 
  * A log file
  * A ``reprodata.out`` file. 
* Open the output file and confirm that your greeting was there! 

If you'd like to name your file something more recognisable, click on the FileDROP and fill in the ``filepath`` parameter. Re-run the steps above and confirm: 

* A new session directory has appeared in the 'workspace' folder
* Your new file with the user-provided name appears with the correct information. 

Basic, Extended: Creating graph with existing applications
==========================================================

Outcomes
--------

* Add App and DataDROPs to the EAGLE canvas
* Update parameters 
* Add Input/OutputPorts to a parameter and experiment with Encoding. 

Steps
------ 
In this example, we are going to do functionally the same thing as above, but we are going to build the graph, so we can get used to the EAGLE canvas, and explore some important EAGLE/DALiuGE concepts. 

#. Create a new graph (Shortcut: "N")
#. Create a HelloWorldApp component on the EAGLE Graph Canvas

  - Hint: You can right click on the canvas and start typing the name of the drop you want, and it will come up as a suggestion. Press the **up/down** arrow keys to select and then **Enter** to add it to the graph. 

#. Click on the HelloWorld and open the **Fields Table** (Shortcut: T)
#. You'll see the ``greet`` parameter, as you did in the previous example - fill this in with your chosen greeting
#. Now, for the n the **Use As** column, click on the drop-down menu for the ``output`` attribute and select **OutputPort**
#

Use the hello world app 

Intermediate: Using the PyFuncApp
==================================

Using the following code: 

.. code-block:: python3 

    def hello_world(greet: str):
        """
         Designed to mimic the functionality of dlg.apps.simple.HelloWorld, which takes
        a 'greet' parameter as input and returns "Hello " + greet.

        :param greet: The 'item' we are greeting.
        :return: str
        """
        return f"Hello, {greet}"


Intermediate: Using the BashShellApp
====================================

Finally, let's run a 'complete' script. This approach is most equivalent to existing systems such as Nextflow, and uses our BashShellApp. 

First

.. code-block:: python3 

  import argparse

  def hello_world(greet: str):
      """
       Designed to mimic the functionality of dlg.apps.simple.HelloWorld, which takes
      a 'greet' parameter as input and returns "Hello " + greet.

      :param greet: The 'item' we are greeting.
      :return: str
      """
      return f"Hello, {greet}"

  if __name__ == '__main__':
      parser = argparse.ArgumentParser()
      parser.add_argument("--greet", type=str, default="World",
                          help="The greeting we want to apply")
      args = parser.parse_args() 
      print(hello_world(args.greet))


Having followed the tutorials above, as with all programming related tutorials, we encourage you to try out a simple "Hello, World" example using a BashShellAppDROP and FileDROP. 

To do this, you will need to: 

#. Create a new graph 
#. Create a BashShellAppDROP component on the EAGLE Graph Canvas
#. Click on the BashShellAppDrop and open the **Fields Table**
#. In the ``command`` row, add the following to the **value** column: ``echo "Hello, World" > {output}``
#. In the **Fields Table**, click **Add Parameter** and give it the name ``output``, and value ``demo.txt``

  * Note: This is using our string-substituion approach ; the parameter name ``output`` and the string inside the braces (`{}`) need to match.
  * Note: The demo.txt could also have been used as part of the command line directly; however, this would fix the name of the file to demo.txt which can be a problem if we want to scale the workflow (more on this in further tutorials).

#. In the **Use As** column, click on the drop-down menu for the ``output`` attribute and select **OutputPort**
#. This should create an OutputPort on the BashShellAppDROP; add an output FileDROP to the BashShellAppDROP by clicking on the new port and dragging, then selecting **Built-In Components -> File**
#. Save the graph, either to a repository or locally (EAGLE will not let you translate without saving).  
#. Start the daliuge-translator and daliuge-engine applications based on your :ref:`current environment<running>`.
#. Click on the **Translate** button, which should open a new tab to show the translated workflow .
#. From this tab, click on the **Deploy** button, which will run the workflow on your locally running DROPManagers. 
#. If all is successful, you will be able to see a file created in your DALiuGE workspace (/home/dlg/workspace by default.). 
#. Open the most recent session directory, and check the output in ``demo.txt``


Conclusion
==========

You are now hopefully a lot more comfortable interacting with the EAGLE interface, and have experience iterating over the Edit->Translate->Deploy development cycle with DALiuGE. 

We recommend starting to build your own graph from either existing code, or from scratch! Please refer to our library of reference graphs 