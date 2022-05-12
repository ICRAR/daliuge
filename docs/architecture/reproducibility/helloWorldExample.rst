.. _reproducibility_helloworld:

Hello World Example
===================
We present a simple example based on several 'Hello world' workflows.
First, we present the workflows and signatures for all rmodes and discuss how they compare.

Hello World Bash
----------------
This workflow is comprised of a bash script, writing text to a file -
Specifically `echo 'Hello World' > %o0`

.. image:: HelloWorldBash.png

Hello World Python
------------------
This workflow is comprised of a single python script and a file.
This function writes 'Hello World' to the linked file.

.. image:: HelloWorldPython.png

Hello Everybody Python
----------------------
This workflow is again comprised of a single python script and file.
This function writes 'Hello Everybody' to the linked file.

.. image:: HelloEverybodyPython.png

Signature Comparisons
---------------------

By comparing the hashes of each workflow together, we arrive at the following conclusions:

.. csv-table:: Workflow Hashes
    :file: HelloHashes.csv
    :widths: 13, 12, 12, 12, 12, 12, 12, 12
    :header-rows: 1

* HelloEverybodyPython and HelloWorldPython are Reruns
* No two workflows are repetitions
* No two workflows are recomputations
* HelloWorldBash and HelloWorldPython reproduce the same results
* No two workflows are replicas.

Testing for repetitions is primarily useful when examining stochastic workflows to take their
results in concert with confidence.
Testing or replicas is useful when moving between deployment environments or verifying the validity
of a workflow.
When debugging a workflow or asserting if the computing environment has changed, recomputations and
computational replicas are of particular use.

This simple example scratches the surface of what is possible with a robust workflow
signature scheme.