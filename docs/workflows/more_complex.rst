.. _more_complex:

Adding complexity to your workflow
==================================


What do we want from this section? 

* We've successful put together and run a workflow
* It didn't _do_ a lot though, did it! 
* Let's do some sort of transformation or manipulation of the data. 
* Let's mix and match some types of apps and data

First we will: 

- Add to the hello world 
- Copy the data around 
- Create a pyfuncapp to do something to that data



- Introduce the more complex workflow that does the gaussian match filtering
- Demonstrate how we can use both the numpy functions to load, and then DALiuGE's writers to write 
- Demonstrate the limitations of this writing approach for some applications
    - Common formats don't necessarily have easy encoders, e.g. FITS HDF5
    - Software support for these encoders require file names
    

Recap
------
- Nods on the graph are DROPS; we have data drops and app drops
- data is stored in (by default) in the $HOME/dlg/workspace directory on a per-session basis; this is referred to as the *session directory*

Reviewing Hello, World
----------------------

Let's drill into the Hello, World example we've run previously. 

.. image:: ../images/workflows/hello_world_basic.png

- Review the fact that Hello, World produces data, and then a file 'appears' on the disk with the output
- This underscores a fundamental part of the Data flow model of DALiuGE - DALiuGE takes control of reading and writing the data

If we run this without any modifications, we end up with a file that has a DALiuGe-defined name, and is stored in the workspace directory. 

Look what happens if we copy the data 
    - Specify an actual file name 
    - Look at the output in the workspace folder 
    
We can also specify a relative filepath if we have a particular name we want to use
    - Click on the ``File`` drop
    - Update the value of the ``filepath`` parameter to ``myfile.txt`` 
      .. image:: ../images/workflows/hello_world_basic_fileattr.png
    - Re-run the graph and check the newest session directory:: 
        
        cd ~/dlg/workspace
        ls -ltr # The bottom result gives you the most recent 
        cd  HelloWorld-simpleXX-XXXX-XX-XXXX-XX-XX.XXXX
        cat myfile.txt

        Hello, DALiuGE


We can also specify an absolute directory path, using environment variables: 
    Update the value of the ``filepath`` parameter to ``$HOME/data/myfile.txt``
    - Re-run the graph and check the ``$HOME/data/`` directory to confirm ``myfile.txt`` is present. 

To recap some invisible but important points here: 
    - Default and preferred DALiuGE behaviour is to be in control of both data read/write, as well as output naming 
    - This is useful because it means in situations where there are loops or scatters, there does not need to be any naming logic between different applications - DALiuGE controls all of this and keeps track of the files without you having to. 
    - Everything takes place within the session directory, meaning that data is localised to a session. This supports reproducibility and improves the isolation of the workflow. 

Extending Hello, World
----------------------

Let's explore how we can extend Hello, World, and show off some more of how DALiUGE manages data 

We will add a new app to the graph that takes the output of Hello, World and reverses the string. 

.. image:: ../images/workflows/hello_world_reverse.png

This contains the following code in the PyFuncApp to reverse anything in the File drop::

    def reverse(greeting: str): 
	return greeting[-1::-1]


Using other encodings, and getting more complex
------------------------------------------------

- Lets do something more astronomical in nature that uses some real libraries! 
- Use .npy loading with the large noise, show how we can write that out as numpy and then make some plots locally

- What's the limitation? 
    - Encoding of the information can be restrictive

We have some array data that represents a noisy signal (:download:`../data/noisy_signal.npy`). Assuming we've downloaded this in a folder like $HOME/data/, lets use ``numpy`` to view the image first to see what we are dealing with::

    >>> import numpy as np 
    >>> np.load("noisy_signal.npy")
    >>> import matplotlib.pyplot as plt
    >>> plt.imshow()
    >>> plt.show()

- Show how we can produce files and switch to memory and back for testing
- Show how we can write without explicitly using a 'writer'

The files produced by this will be in the workspace folder: 

- Check the mask 
- Check the final signal, and we can see there's alternative 


Conclusions
------------


Next
----

In the following section we will 
- Expand more on how data works in DAliuGe and review some best practices
- Use the palette to generate functions 
  
