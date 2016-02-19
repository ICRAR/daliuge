DFMS is the Data Flow Management System prototype for SDP, which represents the Science
Data Process element of the Square Kilometer Array (SKA) observatory. DFMS aims
to provide a distributed data management platform and a
scalable pipeline execution environment to support continuous, soft-real time,
data-intensive processing for producing SKA science ready products. ::

    |   \    | __||  \/  |  / __|     / _|   ___    _ _     / __|   |   \    | _ \
    | |) |   | _| | |\/| |  \__ \    |  _|  / _ \  | '_|    \__ \   | |) |   |  _/
    |___/   _|_|_ |_|__|_|  |___/    |_|    \___/  |_|      |___/   |___/   _|_|_
   _|"""""|_| """ |_|"""""|_|"""""|                         |"""|_|"""""|_| """ |
   "`-0-0-'"`-0-0-'"`-0-0-'"`-0-0-'.                        -0-0-'"`-0-0-'"`-0-0-'

The DFMS development is largely based on SDP requirements, functions and the
overall architecture. Although specifically designed for SDP and SKA,
DFMS has adopted a generic, data-driven framework potentially applicable to
many other data-intensive applications.

DFMS stands on shoulders of many previous studies on dataflow, data
management, distributed systems (databases), graph theory, and HPC scheduling.
DFMS has also borrowed useful ideas from existing dataflow-related open
sources (mostly *Python*!) such as `Luigi <http://luigi.readthedocs.org/>`_,
`TensorFlow <http://www.tensorflow.org/>`_, `Airflow <https://github.com/airbnb/airflow>`_,
`Snamemake <https://bitbucket.org/snakemake/snakemake/wiki/Home>`_, etc.
Nevertheless, we believe DFMS has some unique features well suited
for data-intensive applications:

* Completely data-driven, and data DROP is the graph "node" (no longer just the edge)
  that has persistent states and events
* Integration of data-lifecycle managment within data processing framework
* Separation between logical graphs and physical graphs
* Docker-based processing component interface

In :doc:`overview` we give a glimpse to the main concepts present in DFMS.
Later sections of the documentation describe more in detail how dfms works. Enjoy!
