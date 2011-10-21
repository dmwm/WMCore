#!/usr/bin/env python

"""
I don't know what this is
"""

import unittest
from nose.plugins.attrib import attr
from WMCore.RequestManager.Clipboard.Insert import getRequestsInState

class ClipboardTest(unittest.TestCase):
    """
    I don't know what this does either

    """
    @attr("integration")
    def testA_BaseTest(self):
        reqmgr = "http://vocms144.cern.ch:8687"
        reqs = getRequestsInState(reqmgr, u'running')
        inject(os.environ['COUCHURL'], "opsclip", *reqs)
        return
        

if __name__ == '__main__':
    unittest.main()
    

