#!/usr/bin/env python
"""
_FilesTestCase_

Unit tests for File creation, location and exists, including checks to see that calls 
are database dialect neutral.

"""

__revision__ = "$Id: FakeFeeder_unit.py,v 1.1 2008/07/21 15:43:20 metson Exp $"
__version__ = "$Revision: 1.1 $"

import unittest, logging, os, commands
from WMCore.WMBSFeeder.Fake.Feeder import Feeder as FakeFeeder

class BaseFakeFeederTestCase(unittest.TestCase):
    def setUp(self):
        self.feeder = FakeFeeder(10)
    
    def tearDown(self):
        pass
    
    def testCall(self):
        for i in range(1,20):
            files = self.feeder()
            print i, files
        
        
if __name__ == '__main__':
    unittest.main()