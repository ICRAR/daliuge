# History
https://keepachangelog.com/en/1.0.0/

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
