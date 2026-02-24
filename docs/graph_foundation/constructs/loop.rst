.. _loop:

Loop
################

The loop is the DALiuGE construct that operates like a ``for`` loop in code. The Loop can be used to: 

1. Iterate over an array 
2. Iterate over the computing on some data

Both of these will happen for a maximum `num_of_iter`, which must be set for all loops. Loops that need to exit early can do so with the use of a :ref:`branch`. 

A reference Loop graph that contains all features detailed below, and describes looping over an array, is available `as an example on EAGLE <http://eagle.icrar.org/?service=GitHub&repository=ICRAR/EAGLE-graph-repo&branch=master&path=examples&filename=ArrayLoop.graph>`_.

Supporting attributes
=====================
    
The following key concepts are important to keep in mind when creating a loop: 

Loop Aware
---------------
This is necessary for any edge connecting DROP A, outside the loop, to DROP B, inside loop. This is necessary because by default the translator: 

* Repeats all drops in the loop for ``num_of_iter`` times (i.e. There will be B repeated, say, 10 times (B1, B2,..., B10))
* Duplicate the original edges from the original base DROP (i.e. A->B1, A->B2,...,A->B10). 

Typically, we don't want this behaviour; instead, we want only A->B1, and then B2 will get input from something inside the loop. To make this happen, we need to click on the A->B edge in the EAGLE graph and make it **Loop Aware**. This ensures only the A->B1 edge is created when translating. 

You can confirm that an edge is **Loop Aware** visually as it has a *dotted line*.  


Closes Loop and Group Start/End
-------------------------------
DALiuGE unrolls the loop entirely before execution, so all edges on the execution graph are established during translation. In a loop, we may have no relationship between each iteration - for example, we are operating on a separate data set each loop. However, we need to establish a precedence relationship between the first loop, and the second loop, and so on. 

Thus, DALiuGE needs to know which DROPs in the loop are at the end of the first iteration of the loop, because this is what is connected to the starting application of the next iteration of the loop. If this did not exist, then all iterations of the loop would not wait for the previous iteration (there would be no connection) and they would all start at once. 

The easiest way to do this is to click on the edge that starts/stops this and indicate that the edge **Closes Loop**.  This automatically assigns the Group Start/Group End attributes to the respective nodes. 

You can confirm that an edge **Closes Loop** visually as it has a *dot-dash line*.  

Supporing DROPs
================

The following Apps are useful when considering using loops. 

PickOne
--------

This is used when looping over an array. To use this, keep in mind that:

    - The input and output ports of the PickOne DROP *must* be named ``rest_array``. 
    - The edge between *rest_array* and the PickOne app should usually be marked as **Closes Loop**. 


