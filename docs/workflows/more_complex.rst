.. _more_complex:

More complex workflows
======================


In this section, we'll build on our basic understanding of DALiuGE workflows by adding more complexity and real-world functionality. We'll explore how to manipulate data, combine different types of applications, and work with various data formats.

Our goals are to:

* Extend our simple workflow with more sophisticated data processing
* Learn how to handle different types of data
* Create custom Python functions to transform data
* Understand DALiuGE's data management capabilities

We'll accomplish this by:

1. Building on our Hello World example
2. Learning how to manage data movement between components
3. Creating custom Python applications
4. Working with real astronomical data and common file formats


Reviewing Hello, World
----------------------

Let's drill into the Hello, World example we've run previously, by loading up the HelloWorld-Simple graph (below):

.. image:: ../images/workflows/hello_world_basic.png

If you run the workflow with no changes as before, the output file will have a DALiuGE-defined name and it will be placed in the session workspace directory. This default behaviour is intentional, because it lets DALiuGE guarantee that outputs are tracked consistently and stored in a predictable place without you needing to manually manage filenames. This is an example of a core element of DALiuGE’s dataflow model:

    > DALiuGE is responsibile for reading and writing data, 'allowing' workflow components focus on what they produce

You can then experiment with copying or renaming the output to make this behaviour more visible. For example, we can also specify a relative filepath if we have a particular name we want to use:

    - Click on the ``File`` drop
    - Update the value of the ``filepath`` parameter to ``myfile.txt``

      .. image:: ../images/workflows/hello_world_basic_fileattr.png

    - Re-run the graph and check the newest session directory:: 
        
        cd ~/dlg/workspace
        ls -ltr # The bottom result gives you the most recent 
        cd  HelloWorld-simpleXX-XXXX-XX-XXXX-XX-XX.XXXX
        cat myfile.txt

        Hello, DALiuGE


If you want the output to be stored outside the session directory, you can use an absolute path (including environment variables) such as $HOME or $DATA (if defined on the path).

    - Update the value of the ``filepath`` parameter to ``$HOME/data/myfile.txt``
    - Re-run the graph and check the ``$HOME/data/`` directory to confirm ``myfile.txt`` is present. 

This is particularly helpful for *input* data, which may be stored in a read-only space or a `/scratch/` environment.

To recap some invisible but important points here:

    - The default, and encouraged, DALiuGE behaviour is to be in control of both data read and write, as well as being in control of output file naming:
    - With DALiuGE controlling naming and provenance, individual applications don’t need complicated coordination logic to avoid collisions or track which file belongs to which run.
    - Everything takes place within the *session directory*, meaning that data is localised to a session. This supports reproducibility and improves the isolation of the workflow.

Extending Hello, World
----------------------

Let's explore how we can extend Hello, World, and show off some more of how DALiUGE manages data 

We will add a new app to the graph that takes the output of Hello, World and reverses the string. 

.. image:: ../images/workflows/hello_world_reverse.png

This contains the following code in the PyFuncApp to reverse anything in the File drop::

    def reverse(greeting: str): 
	    return greeting[-1::-1]

Adding this and another output file.

Using other encodings, and getting more complex
------------------------------------------------

We've spent some time with the basic Hello, World example; let's move on to with some real astronomical data processing!

In this example,  we'll create a workflow that demonstrates how to:

* Load and process NumPy array data
* Perform basic signal processing
* Verify the output of the data

For this example, we'll work with a synthetic noisy signal that might represent an astronomical observation. We'll use the NumPy library (which will be installed in your DALiuGE virtual environment).

We have some array data that represents a noisy signal (:download:`noisy_signal.npy <../data/noisy_signal.npy>`). Assuming we've downloaded this in a folder like $HOME/data/, lets use ``numpy`` to view the image first to see what we are dealing with::

    > import numpy as np
    > import matplotlib.pyplot as plt
    > data = np.load("$HOME/data/noisy_signal.npy")
    > plt.imshow(data, cmap='viridis')
    > plt.colorbar()
    > plt.title("Noisy Signal")
    > plt.show()

Let's create a workflow that will:
1. Load this noisy data
2. Apply a simple smoothing filter
3. Save both the filtered result and a mask showing where significant signal was detected

To do this, we create the following graph with PyFuncApps:

.. image:: ../images/workflows/match_filter_graph.png

Reviewing each DROP in the graph:


``signal``
^^^^^^^^^^

Given our assumptions for where we store the data, all we need to do is set the ``filepath`` attribute for the data:

.. image:: ../images/workflows/match_filter_signal_npy.png

``create_matched_filter``
^^^^^^^^^^^^^^^^^^^^^^^^^^

In this DROP, we use some Python code to produce a matched filter of the noisy image, based on techniques presented in Rhodes University's Fundamentals of `Radio Interferometry course <https://github.com/ratt-ru/foi-course/blob/master/5_Imaging/5_A_matched_filter.ipynb>`_

The following code produces a matched filter from the loaded ``.npy`` file, and we put this in the ``func_code`` parameter of the DROP table and put ``create_match_filter` into the ``func_name`` parameter::

    def create_match_filter(noisy_signal, deviation: float, amp:float, theta: float):

        sizex, sizey = noisy_signal.shape

        x0 = sizex/2
        y0 = sizey/2
        norm = amp
        rtheta = theta * 180. / np.pi #convert to radians

        a = (np.cos(rtheta)**2.)/(2.*(deviation**2.)) + (np.sin(rtheta)**2.)/(2.*(deviation**2.))
        b = -1.*(np.sin(2.*rtheta))/(4.*(deviation**2.)) + (np.sin(2.*rtheta))/(4.*(deviation**2.))
        c = (np.sin(rtheta)**2.)/(2.*(deviation**2.)) + (np.cos(rtheta)**2.)/(2.*(deviation**2.))
        gFunc1 = lambda x,y: norm * np.exp(-1. * (a * ((x - x0)**2.) - 2.*b*(x-x0)*(y-y0) + c * ((y-y0)**2.)))

        xpos, ypos = np.mgrid[0:sizex, 0:sizey].astype(float)
        return gFunc1(xpos, ypos)

Then, we need to add inputs and output ports (use the **Add Parameter** button), remembering:

- We are taking a ``numpy`` file as an input, so we can use the built-in `npy` encoding
- We don't particularly care about the output encoding, as we are passing it straight onto the convolution, so don't worry about the encoding here.

The final result will be a table as follows:

.. image:: ../images/workflows/match_filter_create_filter.png

``apply_filter_with_convolution``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To apply the matched-filter onto the recovered signal, we need:

- Two input ports, one for the .npy file, and one for the matched filter
    - The ``match_filter`` should be read in as pickle encoding
    - The signal should be read in as a ``.npy``
- one output port, for the output numpy file.

To apply the convolution, we use the following code and add it to the `apply_filter_with_convolution`` DROP::

    import numpy as np

    def apply_filter_with_convolution(signal, filter):
        return np.fft.ifft2(np.fft.fft2(np.fft.fftshift(filter)) * np.fft.fft2(signal)).real

The final table is going to look like:

.. image:: ../images/workflows/match_filter_apply_convolution.png


``recovered_signal.npy``
^^^^^^^^^^^^^^^^^^^^^^^^^^

To help identify the final image, we can specify a specific output filename:

.. image:: ../images/workflows/match_filter_recovered_signal.png

Reviewing output
------------------------------------------------

The output from this will be in our ``~/dlg/workspace/match_filterX-20XX-XX-XX`` directory, which we can plot and review the image:

    > data = np.load("recovered_signal.npy")
    > plt.imshow(data, cmap='viridis')
    > plt.colorbar()
    > plt.title("Recovered Signal")
    > plt.show()

DALiuGE can load this

- Show how we can produce files and switch to memory and back for testing
- Show how we can write without explicitly using a 'writer'

The files produced by this will be in the workspace folder: 

- Check the mask 
- Check the final signal, and we can see there's alternative 

* DALiuGE can handle NumPy arrays natively through memory


Conclusions
------------

In this tutorial, we've learned:

* How to extend basic workflows with data processing capabilities
* Different ways to manage data movement in DALiuGE
* How to work with both simple text and binary data formats
* Best practices for handling scientific data in workflows

These concepts form the foundation for building more complex scientific workflows. We've seen how DALiuGE can handle both simple string operations and real numerical data processing, making it suitable for a wide range of scientific computing tasks.

However, for some specialized formats (FITS, HDF5), you may need to use custom decoders. Consider memory usage when working with large arrays.

Next
----

In the following sections, we will:

* Take a deeper dive into DALiuGE's data management capabilities
* Learn about best practices for data handling in complex workflows
* Explore how to use the Component Palette to generate template functions
* Build more sophisticated astronomical data processing pipelines

These topics will help you create more efficient and maintainable workflows for your specific use cases.
  
