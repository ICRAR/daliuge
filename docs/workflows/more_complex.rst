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
    

Updating Hello, World
----------------------

Let's drill into the Hello, World example we've run previously. 

- Review the fact that Hello, World produces data, and then a file 'appears' on the disk with the output
- This underscores a fundamental part of the Data flow model of DALiuGE - DALiuGE takes control of reading and writing the data

Look what happens if we copy the data 
    - Specify an actual file name 
    - Look at the output in the workspace folder 

- Use string.reverse to show how data output can be modified
- Show how we can produce files and switch to memory and back for testing 
- Show how we can write without explicitly using a 'writer'

If we start with a PyFuncApp: 

- Using the parallel pi, we can use the copy function 
- We can also start a conversation about using filenames 

- What's the limitation? 
    - Encoding of the information can be restrictive

Next
----
In the following section we will 
- Expand more on how data works in DAliuGe and review some best practices
- Use the palette to generate functions 
  
