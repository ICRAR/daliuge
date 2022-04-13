.. _reproducibility_graphcertification:

Graph Certification
===================
'Certifying' a graph involves generating and publishing reproducibility signatures.
These signatures can be integrated into a CI/CD pipeline, used during executions for verification or
during late-stage development when fine-tuning graphs.

By producing and sharing these signatures, subsequent changes to execution environment, processing
components, overall graph design and data artefacts can be easily and efficiently tested.

Certifying a Graph
------------------
The process of generating and storing workflow signatures is relatively straightforward.

* From the root of the graph-storing directory (usually a repository) create a ``/reprodata/[GRAPH_NAME]`` directory.
* Run the graph with the ``ALL`` reproducibility flag, and move the produced reprodata.out file to the previously created directory.
* (optional) Run from ``dlg.common.reproducibility.reprodata_compare.py`` script with this file as input to generate a summary-csv file

In subsequent executions or during CI/CD scripts:
* Note the reprodata.out file generated during the test execution
* Run ``dlg.common.reproduciblity.reprodata_compare.py`` with the published ``reprodata/[GRAPH_NAME]`` directory and newly generated signature file
* The resulting ``[SESSION_NAME]-comparison.csv`` will contain a simple True/False summary for each RMode, for use at your discretion.

What is to be expected?
***********************
In general, all but ``Recomputation`` and ``Replicate_Computational`` rmodes should match, moreover:

* A failed ``Rerun`` indicates some fundamental structure is different
* A failed ``Repeat`` indicates changes to component parameters or a different execution scale
* A failed ``Recomputation~`` indicates some runtime environment changes have been made
* A failed ``Reproduction`` indicates data artefacts have changed
* A failed ``Scientific Replication`` indicates a change in data artefacts or fundamental structure
* A failed ``Computational Replication`` indicates a change in data artefacts or runtime environment
* A failed ``Total Replica`` indicates a change in data artefacts, component parameters or different execution scale

When attempting to re-create some known graph-derived result, ``Replication`` is the goal.
In an operational context, where data changes constantly, ``Reruning`` is the goal
When conducting science across multiple trials, ``Repeating`` is necessary to use the derived data arte-facts in concert.

Tips on Making Graphs Robust
----------------------------
The most common 'brittle' aspect of graphs are hard-coded paths to data resources and access to referenced data.
This can be ameliorated by:

* Using the ``$DLG_ROOT`` keyword in component parameters as a base path.
* Providing comments on where to find referenced data artefacts
* Providing instructions on how to build referenced runtime libraries (in the case of Dynlib drops).

