# DALiuGE Engine: Test Data

Test data for daliuge-engine/test are stored in the EAGLE_test_data repository (installed
as `daliuge_tests`).


## How to access
```python
from importlib.resources import files
import daliuge_tests.translator as test_graphs

files(test_graphs)
```


## How to add 

Store in the following path: 

```bash
EAGLE_test_repo/eagle_test_graphs/daliuge_tests/dropmake
```


