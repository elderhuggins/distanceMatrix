# distanceMatrix
The first thing you should do is create a file named "local_host.py" in the folder that contains the distanceMatrix folder. The local_host.py file should have code that instantiate your working paths, for example:

```
data_dir = "_data"
import sys, os
sys.path.append('distanceMatrix')
```

This is how you should run the test script from a command prompt. Your working directory should be just above the distanceMatrix directory:
```
python -m distanceMatrix.test_script
```
