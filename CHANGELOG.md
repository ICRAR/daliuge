# History
https://keepachangelog.com/en/1.0.0/

# v6.1.0
- [Feature] Improved daemon support and docker builds: https://github.com/ICRAR/daliuge/pull/365

# v6.0.0
- [Feature] Add support for DirectoryDROP: https://github.com/ICRAR/daliuge/pull/360
  - [Deprecated] This has replaced DirectoryContainerDROP
- [Feature] Add support for GlobalVariableDROP: https://github.com/ICRAR/daliuge/pull/357
  - [Deprecated] This has replaced EnvironmentVarDROP
- [Feature] LIU-457: Add USER log level and error intercept feature: https://github.com/ICRAR/daliuge/pull/344
- [Feature] Add automatic directoryDROP naming: https://github.com/ICRAR/daliuge/pull/364
- [Fixed] Fix pydata issues with Utf-8 pyfunc output: https://github.com/ICRAR/daliuge/pull/363


# v5.6.3
- [Fixed] Revert node_str parameter name in rest.py function definition: https://github.com/ICRAR/daliuge/pull/358

# v5.6.2
- [Fixed] Fix environment variable expansion not working: https://github.com/ICRAR/daliuge/pull/356

## v5.6.1
- [Fixed] Fix parse_pydata and make MemoryDROP Type consistent: https://github.com/ICRAR/daliuge/pull/354

## v5.6.0

- [Added]  HelloWorld Tutorials and Graph Patterns:  https://github.com/ICRAR/daliuge/pull/347
- [Changed] Remove final pkg_resources references from code: https://github.com/ICRAR/daliuge/pull/350

## v5.5.3
- [Fixed] Fix Sessions disappearing in DIM page when restarting Node Manager: https://github.com/ICRAR/daliuge/pull/349

## v5.5.2
- [Fixed] Address issue #315 to make `dlg` remote deployment CLI more clear: https://github.com/ICRAR/daliuge/pull/342
- [Fixed] Fix multi-input  AppDROP failing after Branch False condition: https://github.com/ICRAR/daliuge/pull/346

## v5.5.1
- [Fixed] Update `urllib3` to resolve Dependabot security issue: https://github.com/ICRAR/daliuge/pull/345

## v5.5.0

- [Added] Add experimental support for running server under 'watchdog' process: https://github.com/ICRAR/daliuge/pull/339
- [Added] Add local-time logging option: https://github.com/ICRAR/daliuge/pull/333
- [Changed] Improve CLI clarity and documentation https://github.com/ICRAR/daliuge/pull/335
- [Fixed] Translator re-uses DIM tab on deploy: https://github.com/ICRAR/daliuge/pull/338

## v5.4.0

- [Added] Add named-ports functionality to Branch construct: https://github.com/ICRAR/daliuge/pull/325
- [Changed] Update component docstrings: https://github.com/ICRAR/daliuge/pull/330

## v5.3.1

- [Added] Initial work to build docker images on release: https://github.com/ICRAR/daliuge/pull/312
- [Fixed] Test np.array is empty correctly in named_port_utils.py: https://github.com/ICRAR/daliuge/pull/334

## v5.3.0

- [Added] Per-app logging from the web UI: https://github.com/ICRAR/daliuge/pull/327
- [Added] Change "PythonApp" to "DALiuGEApp": https://github.com/ICRAR/daliuge/pull/326
- [Changed] Update Installation documentation and polish documentation look/structure: https://github.com/ICRAR/daliuge/pull/313

## v5.2.1

- [Fixed] Fix eagle-test-graphs Git branch: https://github.com/ICRAR/daliuge/pull/329
- [Fixed] TestRunner fix failing unittests due to overlapping PR changes: https://github.com/ICRAR/daliuge/pull/328

## v5.2.0

- [Added] FileDROP naming support for PyFuncApp "side effect" files: https://github.com/ICRAR/daliuge/pull/314
- [Fixed] Stopped black boxes and graph zoom when errors occur during Deploy state: https://github.com/ICRAR/daliuge/pull/319
- [Fixed] Added back 'hello' parameter to HelloWorldApp: https://github.com/ICRAR/daliuge/pull/320
- [Deprecated] Removed xml2palette tool from DALiuGE: https://github.com/ICRAR/daliuge/pull/324

## v5.1.0

- [Added] New Branch component based on PyFuncApp that allows for conditional execution: https://github.com/ICRAR/daliuge/pull/317
- [Added] Users can to set the log-level of an application from the graph per-application: https://github.com/ICRAR/daliuge/pull/317
- [Added] Provide ability to read and write plain strings if requested by the user: https://github.com/ICRAR/daliuge/pull/317

## v5.0.0

- [Changed] New BashShellApp command replacement: https://github.com/ICRAR/daliuge/pull/309 
- [Fixed] Improve PyFunc robustness to erroneous graph input: https://github.com/ICRAR/daliuge/pull/308

## v4.9.0

- [Added] Workflow to deploy DALiuGE to PyPI: https://github.com/ICRAR/daliuge/pull/311
- [Added] Added prototype .ini environment config and Slurm template scripts: https://github.com/ICRAR/daliuge/pull/297
- [Added] Private key support for remote submission: https://github.com/ICRAR/daliuge/pull/298
- [Added] Enable per-port serilaisation: https://github.com/ICRAR/daliuge/pull/300
- [Added] Add GraphConfig support to translator: https://github.com/ICRAR/daliuge/pull/296
- [Fixed] Fix writing of BytesIO Data: https://github.com/ICRAR/daliuge/pull/310
- [Fixed] Updated installation documentation: https://github.com/ICRAR/daliuge/pull/307
- [Fixed] Drop naming fix: https://github.com/ICRAR/daliuge/pull/306
- [Fixed] Use translator avahi approach for engine docker container: https://github.com/ICRAR/daliuge/pull/302
- [Fixed] Use correct ports for CompositeManager: https://github.com/ICRAR/daliuge/pull/303


## v4.8.0

- [Added] Support for Python 3.11 and Python 3.12: https://github.com/ICRAR/daliuge/pull/290
- [Added] Session history support in engine through the Composite Managers:  https://github.com/ICRAR/daliuge/pull/291, https://github.com/ICRAR/daliuge/pull/292.
- [Changed] EAGLE_test_repo is now where all test graphs are stored for DALiuGE development: https://github.com/ICRAR/daliuge/pull/286, https://github.com/ICRAR/daliuge/pull/287.
- [Changed] Replaced full UID4s with human-readable keys in graph visualisation and file output: https://github.com/ICRAR/daliuge/pull/293
- [Fixed] Updated the MPIApp and deployment scripts for the Hyades cluster: https://github.com/ICRAR/daliuge/pull/289

## v4.7.3

- [Added] New Node object introduced to allow us to send across all ports information required when running multiple managers, with non-default ports. This fixed existing issue where events could never receive events on a non-default port. 
- [Added] New end-to-end unit testing behaviour to test non-default port behaviour with multiple managers.   

## v4.7.2

- [Changed] Redirected palette generation from EAGLE_test_repo to EAGLE-graph-repo: https://github.com/ICRAR/daliuge/pull/286

## v4.7.1

- [Changed] Modified translator modules to make it easier to refactor; test cases reciprocally updated: https://github.com/ICRAR/daliuge/pull/278
- [Fixed] Fixed `make test` for fresh install, and update ENV_PREFIX, which was relative: https://github.com/ICRAR/daliuge/pull/282

## v4.7.0

- [Added] Support "encoding" attribute in LG schema: https://github.com/ICRAR/daliuge/pull/279, https://github.com/ICRAR/daliuge/pull/284
- [Added] Move from "keys" and "groups" to "*id" in LG schema: https://github.com/ICRAR/daliuge/pull/280
- [Fixed] AVAHI service not starting in translator docker container has been fixed: https://github.com/ICRAR/daliuge/pull/285

## v4.6.0

- [Added] Support for in-line function code in PyFuncApp
- [Added] Support to specify ZMQ and event ports on the dlg command line
- [Fixed] Session log files now include stdout and stderror

## v4.5.0

- [Added] Support for Subgraphs and added example SubGraphLocal AppDROP implementation.

## v4.4.1 

- [Changed] Updated CHANGELOG.md with retroactive version changes from 4.0.2->4.4.0. 

## v4.4.0

- [Added] New Makefile to make building, running, and releasing easier.  

## v4.3.0

- [Added] DataDROPs now support named drop input arguments: https://github.com/ICRAR/daliuge/pull/259

- [Added] Pylint errors are enabled on CI: https://github.com/ICRAR/daliuge/pull/266 

- [Changed] Update attributes in the modelData part of the output palette: https://github.com/ICRAR/daliuge/pull/252

- [Changed] Modify branch doxygen to name output ports 'yes' and 'no': https://github.com/ICRAR/daliuge/pull/252

## v4.2.0

- [Deprecated] Removed support for and implementation of Plasma and PyArrow in daliuge-engine. https://github.com/ICRAR/daliuge/pull/269 

## v4.1.1

- [Changed] `test_pg_gen.py` to compare translation results to test data: https://github.com/ICRAR/daliuge/pull/260 

## v4.1.0

- [Added] Added preliminary support for SubGraph drops to daliuge-translator: https://github.com/ICRAR/daliuge/pull/262 
- [Fixed] Key-word positional now work for all AppDROPS: https://github.com/ICRAR/daliuge/pull/254
- [Fixed] Tests no long produced log spam when run on CI: https://github.com/ICRAR/daliuge/pull/250

## v4.0.3 

- [Fixed] Resolved errors when using Server deploy method in daliuge-engine: https://github.com/ICRAR/daliuge/pull/256
- [Fixed] Added missing rtd_sphinx_theme from documentation dependencies causing build failures: https://github.com/ICRAR/daliuge/pull/257;https://github.com/ICRAR/daliuge/pull/258

## v4.0.2

- [Added] Update `merklelib`to support Python3.10: https://github.com/ICRAR/daliuge/pull/243
- [Fixed] Resolve README.rst and doc failures: https://github.com/ICRAR/daliuge/pull/247
- [Deprecated] Removed Data type from edges in the graph schema

## v4.0.1

- Add avahi-daemon to daliuge-translator image:
- Improvements to named_port_utils.py
    
## v4.0.0 

- All of DALiuGE Development prior to 4.0.0.
