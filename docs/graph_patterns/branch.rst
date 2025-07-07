.. _branch:

Branch
------

The Branch is a conditional construct based on the PyFuncApp DROP. It's main parameters are a function that returns True or False based on requirements for the input data. 

Reasons for using the Branch construct include: 

    - Terminating a Loop when an array is exhausted, before the loop has reached the ``num_iter`` parameter. 
    - Convergence criteria for a value; e.g. leaving a loop once a value is < 0.
