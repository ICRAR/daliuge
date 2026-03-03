.. _workflow_with_files:

Data in DALiuGE 
###############

We have seen in the previous example how data in DALiuGE can be controlled by DALiuGE. In fact, this is the preferred way to manage data in DALiuGE, as it means the DALiUGE engine is completely aware of what is happening in the system during runtime. It also means that unnecessary file system work, such as creating filenaming heuristic and keeping track of filenames, are not necessary; DALiuGE has control over DROP naming, and the user doesn't need to specify any paths (unless they are centralised input data).

Unfortunately, there are a couple of limitations in workflows that DALiuGE must take into account:

- DALiuGE doesn't have support for all input encodings: This would be a significant undertaking, and there will always be a new format or version of a format that DALiuGE would need to support as part of it's DROP parsing. Instead, we pass that responsibility to the user to explicitly draw on the graph when complex data formats (e.g. FITS, HDF5) are being read into the graph.
- Many applications in science lead to :ref:`'side-effects' <side effect>`: A lot of astronomical software, which would be run through a :ref:`dockerapps` or a :ref:`bashapps`, typically takes a file as input and then writes results to an output file. Likewise, many astronomical Python packages have Python functions that require both input and output files specified, and do the writing operations themselves. Running either of these examples within DALiuGE and its default data-management is confusing, because it believes it is in control of the writing of the data.

This section details how you can construct your workflow to support these requirements in DALiuGE.

Format alternatives
===================

Managing side-effects
======================

- Introduce with BashShellApp, which relies on some sort of side effects
    - Link to BashShellApp documentation in reference   
- Introduce formally the path naming approaches.
        - Link to file path reference 

- Show the manipulation of data with PyFuncApps using the match filter exercise
- Show how we can read in data directly using DALiuGE 
