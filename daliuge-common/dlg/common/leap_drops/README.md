# Leap Drops

This directory contains a number of Python classes that expose LEAP functionality to the DALiuGE workflow execution environment. The classes extend the DALiuGE BarrierDropApp class, making them executable as PythonApp drops within the DALiuGE system.

The classes contain custom Doxygen comments that describe the interface to these components through DALiuGE ports and parameters. When changes to the source are pushed to this repository, a Travis CI step generates Doxygen XML for these classes and transforms the XML into an DALiuGE/EAGLE palette file. The palette file can be opened in the EAGLE editor to provide ready-made components for use in DALiuGE workflows.

## ProduceConfig

* Intended for use in a DALiuGE Scatter component
* Loads a CSV file containing directions
* Splits the directions into N groups, where N is the multiplicity of the DALiuGE scatter component
* Produces a JSON config file for each group

## CallLeap

* Loads a JSON config file
* Builds a command line based on the contents of the config file
* Executes LeapAccelerateCLI (or if DEBUG==True, calls Sleep for a short random time)
* Collects the output from LeapAccelerateCLI and writes it to the single output DALiuGE port

## LeapGather

* Intended for use in a DALiuGE Gather component
* Loads the JSON from N instances of CallLeap, where N is the multiplicity of the DALiuGE gather component
* Trivially combines the JSON into a single JSON object
* Sends the combined JSON to the single output DALiuGE port

## Links
* [DALiuGE Application Development](https://daliuge.readthedocs.io/en/latest/writing_an_application.html)
* [EAGLE](http://eagle.icrar.org)
