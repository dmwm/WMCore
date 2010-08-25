#!/usr/bin/python
"""
_FileInfo_t_

General test for FileInfo

"""

__revision__ = "$Id: FileInfo_t.py,v 1.4 2009/10/01 15:15:21 meloam Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "fvlingen@caltech.edu"

import logging
import unittest

from WMCore.FwkJobReport.FileInfo import FileInfo
from WMQuality.TestInit import TestInit

class FileInfoTest(unittest.TestCase):
    """
    A test of a generic exception class
    """    
    def setUp(self):
        """
        setup log file output.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
            
    def tearDown(self):
        """
        nothing to tear down
        """
        
        pass
    
    def testA(self):
        """
        test fileinfo class
        """
        
        f = FileInfo()
        f.addRunAndLumi(2, 1, 2, 3, 4, 5)
        lumis = f.getLumiSections()
        assert lumis == [{'RunNumber': 2, 'LumiSectionNumber': 1}, \
                          {'RunNumber': 2, 'LumiSectionNumber': 2}, \
                          {'RunNumber': 2, 'LumiSectionNumber': 3}, \
                          {'RunNumber': 2, 'LumiSectionNumber': 4}, \
                          {'RunNumber': 2, 'LumiSectionNumber': 5}]


        f2 = FileInfo()
        f2.load(f.save())
        lumis = f2.getLumiSections()
        assert lumis == [{'RunNumber': 2, 'LumiSectionNumber': 1}, \
                         {'RunNumber': 2, 'LumiSectionNumber': 2}, \
                         {'RunNumber': 2, 'LumiSectionNumber': 3}, \
                         {'RunNumber': 2, 'LumiSectionNumber': 4}, \
                         {'RunNumber': 2, 'LumiSectionNumber': 5}]


        f3 = FileInfo()
        f3.addRunAndLumi(1)


        f4 = FileInfo()
        f4.load(f3.save())
        lumis = f4.getLumiSections()
        assert lumis == []
            
if __name__ == "__main__":
    unittest.main()     
