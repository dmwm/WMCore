'''n
Created on Aug 6, 2009

@author: meloam
'''
import unittest
import WMCore.Services.Requests as Requests
from sets import Set
from WMCore.DataStructs.Run import Run
class testJSONRequests(unittest.TestCase):
    def roundTrip(self,data):
        encoded = self.request.encode(data)
        decoded = self.request.decode(encoded)
        self.assertEqual( data, decoded )
        
    def setUp(self):
        self.request = Requests.JSONRequests()
    
    def testSet1(self):
        self.roundTrip(Set([]))
    def testSet2(self):
        self.roundTrip(Set([1,2,3,4,Run(1)]))
    def testSet3(self):
        self.roundTrip(Set(['a','b','c','d']))
    def testSet4(self):
        self.roundTrip(Set([1,2,3,4,'a','b']))
    def testRun1(self):
        self.roundTrip(Run(1))
    def testRun2(self):
        self.roundTrip(Run(1,1))
    def testRun3(self):
        self.roundTrip(Run(1,2,3))
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()