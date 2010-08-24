import unittest

class Test:

    def __init__(self, tests):
        self.tests = tests


    def run(self):
        testSuite = unittest.TestSuite()
        for test in self.tests:
            test[0].developer = test[1]
            testSuite.addTest(test[0])
        testResult = unittest.TestResult()
        self.testResult = testSuite.run(testResult)
 
    def summaryText(self):
        """
        Summary for the tests result.
        """
        print "********************REPORT********************"
        print "*******************FAILURES*******************"
    
        for i in self.testResult.failures:
           obj,msg=i
           print('==============================')
           print(obj.developer+'--->'+obj.__class__.__name__)
           print('==============================')
           print(str(msg))
    
    
        print "*******************ERRORS********************"
    
        for i in self.testResult.errors:
           obj,msg=i
           print('==============================')
           print(obj.developer+'--->'+obj.__class__.__name__)
           print('==============================')
           print(str(msg))

        print "*******************SUMMARY*********************"
        print "Number of tests run: "+str(self.testResult.testsRun)
        print "Number of failures:  "+str(len(self.testResult.failures))
        print "Number of errors  :  "+str(len(self.testResult.errors))

