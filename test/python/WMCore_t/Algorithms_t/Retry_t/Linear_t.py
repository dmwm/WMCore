import unittest

from WMCore.Algorithms.Retry.Basic import RetryFailException
from WMCore.Algorithms.Retry.Linear import Linear

class DummyClass:
    def __init__(self, max):
        self.count = 0
        self.max = max
        
    def function(self, *args, **kwargs):
        self.count += 1
        if self.count == self.max:
            return True
        else:
            raise 
        
class BasicTest(unittest.TestCase):
    def testBasicSuccesss(self):
        # This'll work on the 5th attempt 
        dmy = DummyClass(2)
        # use miliseconds so the tests don't take forever...
        rty = Linear(max = 3, unit = 1/1000)
        rty.run(dmy.function)
    
    def testBasicFail(self):
        # This'll work on the 15th attempt, which means the test will throw here
        dmy = DummyClass(15)
        # use miliseconds so the tests don't take forever...
        rty = Linear(max = 3, unit = 1/1000)
        self.assertRaises(RetryFailException, rty.run, dmy.function)
    
if __name__ == "__main__":
    unittest.main() 