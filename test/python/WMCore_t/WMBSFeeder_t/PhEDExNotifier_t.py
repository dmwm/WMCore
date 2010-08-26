#!/usr/bin/env python
"""
_FilesTestCase_

Unit tests for File creation, location and exists, including checks to see that calls 
are database dialect neutral.

"""

__revision__ = "$Id: PhEDExNotifier_t.py,v 1.3 2008/11/28 13:31:37 gowdy Exp $"
__version__ = "$Revision: 1.3 $"

import unittest, logging, os, commands
from WMCore.WMBSFeeder.PhEDExNotifier.PhEDExNotifierComponent import PhEDExNotifierComponent
from WMCore.DataStructs.Fileset import Fileset

class PhEDExNotifierTest(unittest.TestCase):
    def setUp(self):
        nodeList = [ "T0_CH_CERN_MSS", "T2_CH_CAF" ]
        self.feeder = PhEDExNotifierComponent( nodeList )
    
    def tearDown(self):
        pass
    
    def testCall(self):
        block = Fileset(name="/HCALNZS/CSA08_STARTUP_V2_v2/RECO#2d041209-fff6-4a71-81fa-2e4b155ed92b")
        self.callPhEDExNotifier( block )
        dataset = Fileset(name="/GammaJets/CSA08_1PB_V2_RECO_EcalCalElectron_v1/ALCARECO")
        self.callPhEDExNotifier( dataset )

    def callPhEDExNotifier( self, fileset ):
        fileset.commit()
        print "iteration 0: %s new files (%s total)" % (len(fileset.listNewFiles()), len(fileset.listFiles())) 
        for i in range(1, 4):
            self.feeder([fileset])
            print "iteration %s: %s new files (%s total)" % (i, len(fileset.listNewFiles()), len(fileset.listFiles())) 
            set = fileset.listFiles()
            if len(set) > 0:
                file = set.pop()
                print file["locations"], file["lfn"]        
            fileset.commit()
    
if __name__ == '__main__':
    unittest.main()
