#!/usr/bin/env python
"""
_FilesTestCase_

Unit tests for File creation, location and exists, including checks to see that calls
are database dialect neutral.

"""




import unittest, logging, os, commands
from WMCore.WMBSFeeder.Fake.Feeder import Feeder as FakeFeeder
from WMCore.WMBSFeeder.Fake.UpdatingFeeder import Feeder as UpdatingFakeFeeder
from WMCore.DataStructs.Fileset import Fileset

class BaseFakeFeederTestCase(unittest.TestCase):
    def setUp(self):
        self.feeder = FakeFeeder(10)

    def tearDown(self):
        pass

    def testCall(self):
        fileset = Fileset(name="FakeFeederTest")
        for i in range(1, 21):
            self.feeder([fileset])
            set = fileset.getFiles(type = "set")
            if len(set) > 0:
                file = set.pop()
            fileset.commit()

class BaseUpdatingFakeFeederTestCase(BaseFakeFeederTestCase):
    def setUp(self):
        self.feeder = UpdatingFakeFeeder(10, 10)


if __name__ == '__main__':
    unittest.main()
