#!/usr/bin/env python
"""
_File_

Unittest for the WMCore.DataStructs.File class

"""


# This code written as the first real test I've written.
# -mnorman


import unittest
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run


class FileTest(unittest.TestCase):
    """
    _FileTest_

    """


    def testDefinition(self):
        """
        This tests the definition of a DataStructs File object

        """

        testLFN     = "lfn"
        testSize    = "1024"
        testEvents  = "100"
        testCksum   = {"cksum": "1"}
        testParents = "parent"

        testFile = File(lfn = testLFN, size = testSize, events = testEvents, checksums = testCksum, parents = testParents)

        self.assertEqual(testFile['lfn'],     testLFN)
        self.assertEqual(testFile['size'],    testSize)
        self.assertEqual(testFile['events'],  testEvents)
        self.assertEqual(testFile['checksums'],   testCksum)
        self.assertEqual(testFile['parents'], testParents)


        return

    def testAddRun(self):
        """
        This tests the addRun() function of a DataStructs File object

        """

        testLFN     = "lfn"
        testSize    = "1024"
        testEvents  = "100"
        testCksum   = "1"
        testParents = "parent"

        testLumi      = 1
        testRunNumber = 1000000

        testFile = File(lfn = testLFN, size = testSize, events = testEvents, checksums = testCksum, parents = testParents)
        testRun = Run(testRunNumber, testLumi)

        testFile.addRun(testRun)

        assert testRun in testFile['runs'], "Run not added properly to run in File.addRun()"

        return


    def testSaveAndLoad(self):
        """
        This tests the save and load code of a DataStructs File object
        This has yet to be implemented

        """

        #It has been noted in the comments in DataStructs/File.py that
        #the save and load functions exist only to be overridden by
        #descendents of the DataStruct/File object, so I am not
        #testing this functionality.  This is just a placeholder in
        #case those requirements change.

        return
    


if __name__ == '__main__':
    unittest.main()
