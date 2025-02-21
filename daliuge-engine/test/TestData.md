# DALiuGE Engine: Test Data

Test data for daliuge-engine/test are stored in the EAGLE_test_data repository (installed
as `daliuge_tests`).


## How to access
```python
from importlib.resources import files
import daliuge_tests.engine.graphs as test_graphs


files(test_graphs)
```
The engine is concerned with pre-translated graphs, so the "test_graph.graph" file will 
typically be a Phyiscal Graph Template, not a Logical Graph. 

## How to add 

Store in the following path: 

```bash
EAGLE_test_repo/eagle_test_graphs/daliuge_tests/engine/graphs
```