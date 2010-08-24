#!/usr/bin/env python
"""
_FilesTestCase_

Unit tests for File creation, location and exists, including checks to see that calls 
are database dialect neutral.

"""

__revision__ = "$Id: PhEDExNotifier_t.py,v 1.2 2008/11/28 10:15:54 gowdy Exp $"
__version__ = "$Revision: 1.2 $"

import unittest, logging, os, commands
from WMCore.WMBSFeeder.PhEDExNotifier.PhEDExNotifierComponent import PhEDExNotifierComponent
from WMCore.DataStructs.Fileset import Fileset

class BasePhEDExNotifierComponentTestCase(unittest.TestCase):
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
        for i in range(1, 4):
            fileset.commit()
            print "iteration %s: %s new files (%s total)" % (i, len(fileset.listNewFiles()), len(fileset.listFiles())) 
            self.feeder([fileset])
            set = fileset.listFiles()
            if len(set) > 0:
                file = set.pop()
                print file["locations"], file["lfn"]        
        # I do this outside the loop as I moved this to the start of the
        # loop so I could do the print out before the first call to
        # PhEDExNotifier
        fileset.commit()
    
if __name__ == '__main__':
    unittest.main()
