#!/usr/bin/env python


import os
import inspect
import unittest


class ThisModule:
    pass

thisFile = inspect.getsourcefile(ThisModule)
thisDir = os.path.abspath(os.path.dirname(thisFile))
testmodules = [ x for x in os.listdir(thisDir) if x.endswith("_t.py")]

result = unittest.TestResult()
loader = unittest.TestLoader()
for testmod in testmodules:
    modName = testmod.replace(".py", "")
    suite = loader.loadTestsFromName(modName)
    suite.run(result)


print result

