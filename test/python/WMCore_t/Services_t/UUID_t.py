#!/bin/env python





from __future__ import print_function
from builtins import str

import unittest
import time

from WMCore.Services.UUIDLib import makeUUID

class UUIDTest(unittest.TestCase):


    def setUp(self):
        pass

    def tearDown(self):
        pass


    def testUUID(self):

        listOfIDs = []

        tmpID = makeUUID()
        splitID = None
        splitID = tmpID.split('-')

        for i in range(0,1000):
            tmpID = makeUUID()
            tmpSplit = tmpID.split('-')
            self.assertNotEqual(tmpSplit[1], splitID[1], "Second component of UUID the same %s != %s"
                             % (tmpSplit[1], splitID[1]))
            self.assertNotEqual(tmpSplit[4], splitID[4], "Fourth component of UUID the same %s != %s"
                             % (tmpSplit[4], splitID[4]))
            self.assertEqual(type(tmpID), str)
            self.assertEqual(listOfIDs.count(tmpID), 0, "UUID repeated!  %s found in list %i times!"
                             % (tmpID, listOfIDs.count(tmpID)))
            listOfIDs.append(tmpID)



        return


    def testTime(self):

        nUIDs     = 100000
        startTime = time.time()
        for i in range(0,nUIDs):
            makeUUID()
        print("We can make %i UUIDs in %f seconds" %(nUIDs, time.time() - startTime))

if __name__ == '__main__':
    unittest.main()
