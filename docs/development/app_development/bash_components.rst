.. _bash_components:

Bash Components
===============
These are probably the easiest components to implement and for simple ones it is possible to do all the 'development' in EAGLE.

'Hello World' in Bash through EAGLE
-----------------------------------
Steps

* Open EAGLE (e.g. https://eagle.icrar.org) and create a new graph (Graph --> New --> Create New Graph)
* Drag and drop a 'Bash Shell App' from the 'All Nodes' palette on the right hand panel onto the canvas.
* Click on the 'Bash Shell App' title bar in the Inspector tab on the right hand panel. This will open all additional settings below.
* First change the 'Name' field of the app in the 'Display Options' menu. Call it 'Hello Word'. Once you leave the entry field also the black title bar will reflect that new name.
* Now change the description of the app in the 'Description' menu. Maybe you write 'Simple Hello World bash app'.
* now go down to the 'Component Parameters' menu and enter the bash command in the 'Command' field::
 
    echo "Hello World"  

* Now save your new toy graph (Graph --> Local Storage --> Save Graph).

That should give you the idea how to use bash commands as |daliuge| components. Seems not a lot? Well, actually this is allowing you to execute whatever can be executed on the command line where the engine is running as part of a |daliuge| graph. That includes all bash commands, but also every other executable available on the PATH of the engine. Now that is a bit more exciting, but the excitement stops as soon as you think about real world (not Hello World) examples: Really useful commands will require inputs and outputs in the form of command line parameters and files or pipes. This is discussed in the :ref:`advanced_bash` chapter. 

Verification
------------

Do we believe that this is actually really working? Well, probably not. Thus let's just translate and execute this graph. Note that the graph has neither an input nor an output defined, thus there is nothing you could really expect from running it. However, the |daliuge| engine is pretty verbose when run in debug mode and thus we will use that to investigate what's happening. The following steps are very helpful when it comes to debugging actual components.

Assuming you have a translator and an engine running you can actually translate and execute this, pretty useless, graph. If you have the engine running locally in development mode, you can even see the output in the session log file::

    cd /tmp/dlg/logs
    ls -ltra dlg_*

The output of the ls command looks like::

    -rw-r--r-- 1 root root 1656 Sep 14 16:46 dlg_172.17.0.3_Diagram-2021-09-14-16-41-283_2021-09-14T08-46-17.341082.log
    -rw-r--r-- 1 root root 6991 Sep 14 16:46 dlg_172.17.0.3_Diagram-2021-09-14-16-41-284_2021-09-14T08-46-52.618798.log
    -rw-r--r-- 1 root root 6991 Sep 14 16:47 dlg_172.17.03_Diagram-2021-09-14-16-41-284_2021-09-14T08-47-28.890072.log

There could be a lot more lines on top, but the important one os the last line, which is the log-file of the session last executed on the engine. Just dump the content to the screen in a terminal::

    cat dlg_172.17.03_Diagram-2021-09-14-16-41-284_2021-09-14T08-47-28.890072.log

Since the engine is running in debugging mode there will be many lines in this file, but towards the end you will find something like::

    2021-09-14 08:47:28,912 [ INFO] [      Thread-62] [2021-09-14] dlg.apps.bash_shell_app#_run_bash:217 Finished in 0.006 [s] with exit code 0
    2021-09-14 08:47:28,912 [DEBUG] [      Thread-62] [2021-09-14] dlg.apps.bash_shell_app#_run_bash:220 Command finished successfully, output follows:
    ==STDOUT==
    Hello World

    2021-09-14 08:47:28,912 [DEBUG] [      Thread-62] [2021-09-14] dlg.manager.node_manager#handleEvent:65 AppDrop uid=2021-09-14T08:46:48_-1_0, oid=2021-09-14T08:46:48_-1_0 changed to execState 2
    2021-09-14 08:47:28,912 [DEBUG] [      Thread-62] [2021-09-14] dlg.manager.node_manager#handleEvent:63 Drop uid=2021-09-14T08:46:48_-1_0, oid=2021-09-14T08:46:48_-1_0 changed to state 2

In addition to the session log file the same information is also contained in the dlgNM.log file in the same directory. That file contains all logs produced by the node manager for all sessions and more, which is usually pretty distracting. However, the name of the session logs are not known before you deploy the session and thus another trick is to monitor the dlgNM.log using the tail command::

    tail -f dlgNM.log

When you now deploy the graph again and watch the terminal output, you will see a lot of messages pass through.

.. _advanced_bash:

Advanced Bash Components
------------------------
.. include:: bash_advanced.rst