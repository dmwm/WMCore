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
        testFile = File()
        self.assertEqual(testFile['lfn'], "")
        self.assertEqual(testFile['size'], 0)
        self.assertEqual(testFile['events'], 0)
        self.assertEqual(testFile['checksums'], {})
        self.assertItemsEqual(testFile['parents'], {})
        self.assertItemsEqual(testFile['locations'], {})
        self.assertFalse(testFile['merged'])

        param = {"lfn": "my_lfn",
                 "size": 1024,
                 "events": 100,
                 "checksums": {'adler32': 'BLAH', 'cksum': '12345'},
                 "parents": "my_parent",
                 "locations": {"PNN_Location"},
                 "merged": True}
        testFile = File(lfn=param['lfn'], size=param['size'], events=param['events'], checksums=param['checksums'],
                        parents=param['parents'], locations=param['locations'], merged=param['merged'])

        self.assertEqual(testFile['lfn'], param['lfn'])
        self.assertEqual(testFile['size'], param['size'])
        self.assertEqual(testFile['events'], param['events'])
        self.assertItemsEqual(testFile['checksums'], param['checksums'])
        self.assertEqual(testFile['parents'], param['parents'])
        self.assertItemsEqual(testFile['locations'], param['locations'])
        self.assertTrue(testFile['merged'])

        return

    def testAddRun(self):
        """
        This tests the addRun() function of a DataStructs File object

        """

        testLFN = "lfn"
        testSize = "1024"
        testEvents = "100"
        testCksum = "1"
        testParents = "parent"

        testLumi = 1
        testRunNumber = 1000000

        testFile = File(lfn=testLFN, size=testSize, events=testEvents, checksums=testCksum, parents=testParents)
        testRun = Run(testRunNumber, testLumi)

        testFile.addRun(testRun)

        assert testRun in testFile['runs'], "Run not added properly to run in File.addRun()"

        return

    def testSaveAndLoad(self):
        """
        This tests the save and load code of a DataStructs File object
        This has yet to be implemented

        """

        # It has been noted in the comments in DataStructs/File.py that
        # the save and load functions exist only to be overridden by
        # descendents of the DataStruct/File object, so I am not
        # testing this functionality.  This is just a placeholder in
        # case those requirements change.

        return

    def testSetLocation(self):
        """
        Test the `setLocation` method functionality
        """
        testFile = File(lfn="test_file")
        self.assertItemsEqual(testFile['locations'], {})

        testFile.setLocation(None)
        self.assertItemsEqual(testFile['locations'], {})

        testFile.setLocation("")
        self.assertItemsEqual(testFile['locations'], {})

        testFile.setLocation([])
        self.assertItemsEqual(testFile['locations'], {})

        testFile.setLocation("valid_PNN")
        self.assertItemsEqual(testFile['locations'], {"valid_PNN"})


if __name__ == '__main__':
    unittest.main()
