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
  
  dlg tm -t /tmp/ -d /tmp/

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
#. Now, click on the ``OutputPort``, (the white dot on the HelloWorldApp) and drag the mouse out from the node, then let go. This will open up another search box - type "file" and select ``FileDROP`` from the options. 

This should put us in a similar state to the original graph. Re-run the Translate-Deploy workflow and open the new output file. 

Intermediate: Using the PyFuncApp
==================================

Outcomes
--------

* Understand PyFuncApp
* Use the code/ directory
* Experiment with ``encoding`` options

Steps
-----

One of the most powerful features of DALiuGE is its ability to run Python code directly from the graph - there's no need to even put it in a script! 

We will start with some existing code (which you are welcome to modify!). 

.. code-block:: python3 

    def hello_world(greet: str):
        """
         Designed to mimic the functionality of dlg.apps.simple.HelloWorld, which takes
        a 'greet' parameter as input and returns "Hello " + greet.

        :param greet: The 'item' we are greeting.
        :return: str
        """
        return f"Hello, {greet}"

Please copy the code into a new file called ``pyfunc.py`` and save it in the ``dlg/code`` directory. This directory is searched at runtime by the DALiuGE engine, so code imports can be stored relative to here. 

Now, in a new canvas, create a PyFuncApp using the right-click and search method. Click on the new AppDROP and inspect the table: 

- You'll see a parameter called ``func_name``; this is where you should put name function based on its importable name. For our example, this would be ``pyfunc.hello_world``. 
- The PyFuncApp currently has no Input or OutputPort. A template PyFuncApp also does not start with an output. 
  - Create a new parameter (using the 'Add Parameter' button at the top of the attribute table). 
  - Give a name to this output (e.g. ``output``)
  - In the **Use As** column, click on the drop-down menu for the ``output`` attribute and select **OutputPort**. 
- Now you can do the same as our previous example and make a ``FileDROP`` from the OutputPort.
- Translate and deploy the graph, open the Session folder, and open the new file

* You will probably get a message about the encoding not being correct; or, it will look like some random characters are in the file, possibly with some part of the message you typed in. 

This is an opportunity to talk about DALiuGE encoding. The output of an AppDROP may need to be represented in various ways, including: 

- The data goes from one AppDROP to another AppDROP; here, the encoding does not matter (default will be **pickle**)
- The data goes from an AppDROP to a file; here, if we expect the file to be human-readable, we would want this 'encoded' as **utf-8**. 
- The data goes from an AppDROP to a file, and we have another AppDROP that wants to read the data in the File; here, the encoding does not matter (default will be **pickle**)
- The data goes from an AppDROP to a file, and we have another AppDROP that wants the *filename*; here, we need to use the **path** encoding, otherwise DALiuGE will attempt to read the data from the file. 

These are just some examples. For the purpose of this tutorial, we just want you to be aware that picking the correct encoding is important if you are trying to avoid surprises! 

In this example, we want to pick *utf-8*; once doing so, re-translate and deploy the graph and confirm that you do get something legible! 


Intermediate: Using the BashShellApp
====================================

Outcomes
--------

* Use DALiuGE with a script
* Understand the limitations of Bash approach 

Steps
-----

Finally, let's run a 'complete' script. This approach is most equivalent to existing systems such as Nextflow, and uses our BashShellApp. 

First, we will update the existing `pyfunc.py` code to be more akin to an actual script that you would call with command-line arguments: 

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


Next, using the tools you are famililar with now: 

#. Create a new graph 
#. Create a BashShellAppDROP component on the EAGLE Graph Canvas (hint: right-click and search)
#. Click on the BashShellAppDrop and open the **Fields Table** (T)
#. In the ``command`` row, add the following to the **value** column: ``python3 $DLG_ROOT/code/hello_world.py --greet ICRAR {filepath}``
#. In the **Fields Table**, click **Add Parameter** and give it the name ``filepath``

  - Note: This is using our string-substituion approach ; use the parameter name ``filepath`` and the string inside the braces (`{}`) need to match.
  - In the **Use As** column, click on the drop-down menu for the ``filepath`` attribute and select **OutputPort**
  - Note: for this, the ``filepath`` is the output that we are producing as a side-effect of our BashShellApp. Therefore, we want the FileDROP to be linked to it by the ``path``, not the data. This means we want to select the ``path`` encoding. 
  - For the `FileDrop`, you can add a name to the file as we did in our Basic example (e.g. ``bashapp.txt``). The BashShellApp will query the FileDROP at runtime and check if it has a Filename, and then use that as the output path for the string replacement in the Bash redirect {filepath}.  
  - *Warning: There is a known issue in EAGLE that can lead to two ``filepath`` attributes being created. Ensure you delete the one that is not connected to an InputPort.*

#. You should now have an OutputPort on the BashShellAppDROP; create a FileDROP to the BashShellAppDROP by clicking on the new port and dragging, then searching for ``File``
#. Translate and deploy
#. If all is successful, you will be able to see a file created in your DALiuGE workspace (/home/dlg/workspace by default.). 
#. Open the most recent session directory, and check the output in ``bashapp.txt``

Conclusion
==========

You are now hopefully a lot more comfortable interacting with the EAGLE interface, and have experience iterating over the Edit->Translate->Deploy development cycle with DALiuGE. 

We recommend starting to build your own graph from either existing code, or from scratch! Please refer to our library of reference graphs 