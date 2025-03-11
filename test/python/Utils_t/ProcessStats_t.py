#!/usr/bin/env python
"""
Unittests for ProcessStats module
"""

# system modules
import unittest
import os

# WMCore modules
from Utils.ProcessStats import processThreadsInfo


class TestProcessTHreadsInfo(unittest.TestCase):
    """
    TestProcessThreadsInfo unit test class
    """
    def testProcessThreadsInfo(self):
        pid = os.getpid()  # Get current process ID
        info = processThreadsInfo(pid)
        self.assertTrue(info['pid'] == pid)
        self.assertTrue(info['status'] == 'running')
        self.assertTrue(len(info['threads']) == 0)


if __name__ == "__main__":
    unittest.main()
