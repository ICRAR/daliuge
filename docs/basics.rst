.. _basics:

Moving Forward 
---------------

Having successfully established running installation of DALiuGE applications, it is now possible to begin testing a new workflow!

To continue working on a new workflow, it is recommended to take the following steps:

* EAGLE: The editor provides three sets of interactive tutorials in-browser. It is *highly* recommended you follow those tutorials. Go to https://eagle.icrar.org and navigate to the Help > Tutorials section on the navigation bar to review them. 
* dlg_paletteGen: This allows you to create and load Python modules as Palettes into EAGLE, and begin creating workflows directly from Python classes, methods, and functions. 


"Hello DALiuGE"
---------------
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

