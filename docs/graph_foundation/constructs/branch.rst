.. _branch:

Branch
------

The Branch is a conditional construct based on the PyFuncApp DROP. It's main parameters are a function that returns True or False based on requirements for the input data. 

Reasons for using the Branch construct include: 

    - Terminating a Loop when an array is exhausted, before the loop has reache the ``num_iter`` parameter. An example of this approach is available `on EAGLE <http://localhost:8888/?service=GitHub&repository=ICRAR/dlg-reference-graphs&branch=main&path=graph_patterns/branch&filename=Branch_ArrayLoopExit.graph>`_.
    - Convergence criteria for a value; e.g. leaving a loop once a value is \< 0. (Note: this can be acheived using exactly the same pattern, just with an alternative 'test' condition.) See an example `using EAGLE <http://localhost:8888/?service=GitHub&repository=ICRAR/dlg-reference-graphs&branch=main&path=graph_patterns/branch&filename=Branch_LoopConditionExit.graph>`_.



