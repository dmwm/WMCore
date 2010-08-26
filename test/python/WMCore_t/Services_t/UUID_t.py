#!/bin/env python

__revision__ = "$Id: UUID_t.py,v 1.1 2009/10/29 20:51:55 mnorman Exp $"
__version__ = "$Revision: 1.1 $"


import unittest
import os
import logging
import socket
import time

from WMCore.Services.UUID import makeUUID

class UUIDTest(unittest.TestCase):


    def setUp(self):
        pass

    def tearDown(self):
        pass


    def testUUID(self):

        listOfIDs = []

        splitID = None

        for i in range(0,1000):
            tmpID = makeUUID()
            if not splitID:
                splitID = tmpID.split('-')
            tmpSplit = tmpID.split('-')
            self.assertEqual(tmpSplit[1], splitID[1], "Second component of UUID not the same %s != %s" \
                             %(tmpSplit[1], splitID[1]))
            self.assertEqual(tmpSplit[2], splitID[2], "Third component of UUID not the same %s != %s" \
                             %(tmpSplit[2], splitID[2]))
            self.assertEqual(tmpSplit[4], splitID[4], "Fourth component of UUID not the same %s != %s" \
                             %(tmpSplit[4], splitID[4]))
            self.assertEqual(type(tmpID), str)
            self.assertEqual(listOfIDs.count(tmpID), 0, "UUID repeated!  %s found in list %i times!" %(tmpID, listOfIDs.count(tmpID)))
            listOfIDs.append(tmpID)



        return


    def testTime(self):

        nUIDs     = 100000
        startTime = time.clock()
        for i in range(0,nUIDs):
            makeUUID()
        print "We can make %i UUIDs in %f seconds" %(nUIDs, time.clock() - startTime)

        
if __name__ == '__main__':
    unittest.main()
