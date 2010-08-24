#!/usr/bin/env python
"""
_SiteLocalConfig_t_

Unit test for parsing the site local config file.
"""




import os
import unittest

from WMQuality.TestInit import TestInit
from WMQuality.TestInit import requiresPython26

from WMCore.WMInit import getWMBASE

from WMCore.Storage.SiteLocalConfig import SiteLocalConfig

class SiteLocalConfigTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @requiresPython26
    def testFNALSiteLocalConfig(self):
        """
        _testFNALSiteLocalConfig_

        Verify that the FNAL site config file is parsed correctly.
        """
        fnalConfigFileName = os.path.join(getWMBASE(),
                                          "test/python/WMCore_t/Storage_t",
                                          "T1_US_FNAL_SiteLocalConfig.xml")

        mySiteConfig = SiteLocalConfig(fnalConfigFileName)

        assert mySiteConfig.siteName == "T1_US_FNAL", \
               "Error: Wrong site name."

        assert len(mySiteConfig.eventData.keys()) == 1, \
               "Error: Wrong number of event data keys."
        assert mySiteConfig.eventData["catalog"] == "trivialcatalog_file:/uscmst1/prod/sw/cms/SITECONF/T1_US_FNAL/PhEDEx/storage.xml?protocol=dcap", \
               "Eroror: Event data catalog is wrong."

        goldenServers = ["http://cmsfrontier.cern.ch:8000/FrontierInt",
                         "http://cmsfrontier.cern.ch:8000/FrontierInt",
                         "http://cmsfrontier1.cern.ch:8000/FrontierInt",
                         "http://cmsfrontier2.cern.ch:8000/FrontierInt",
                         "http://cmsfrontier3.cern.ch:8000/FrontierInt",
                         "http://cmsfrontier4.cern.ch:8000/FrontierInt"]
        for frontierServer in mySiteConfig.frontierServers:
            assert frontierServer in goldenServers, \
                   "Error: Unknown server: %s" % frontierServer
            goldenServers.remove(frontierServer)

        assert len(goldenServers) == 0, \
               "Error: Missing frontier servers."

        goldenProxies = ["http://cmsfrontier1.fnal.gov:3128",
                         "http://cmsfrontier2.fnal.gov:3128",
                         "http://cmsfrontier3.fnal.gov:3128",
                         "http://cmsfrontier4.fnal.gov:3128"]
        for frontierProxy in mySiteConfig.frontierProxies:
            assert frontierProxy in goldenProxies, \
                   "Error: Unknown proxy: %s" % frontierProxy
            goldenProxies.remove(frontierProxy)

        assert len(goldenProxies) == 0, \
                "Error: Missing proxy servers."

        assert mySiteConfig.localStageOut["se-name"] == "cmssrm.fnal.gov", \
               "Error: Wrong se name from local stageout."
        assert mySiteConfig.localStageOut["command"] == "dccp-fnal", \
               "Error: Wrong stage out command."
        assert mySiteConfig.localStageOut["catalog"] == "trivialcatalog_file:/uscmst1/prod/sw/cms/SITECONF/T1_US_FNAL/PhEDEx/storage.xml?protocol=srmv2", \
               "Error: TFC catalog is not correct."

        assert mySiteConfig.fallbackStageOut == [], \
               "Error: Fallback config is incorrect."
        return

    @requiresPython26
    def testVanderbiltSiteLocalConfig(self):
        """
        _testFNALSiteLocalConfig_

        Verify that the FNAL site config file is parsed correctly.
        """
        vandyConfigFileName = os.path.join(getWMBASE(),
                                           "test/python/WMCore_t/Storage_t",
                                           "T3_US_Vanderbilt_SiteLocalConfig.xml")

        mySiteConfig = SiteLocalConfig(vandyConfigFileName)

        assert mySiteConfig.siteName == "T3_US_Vanderbilt", \
               "Error: Wrong site name."

        assert len(mySiteConfig.eventData.keys()) == 1, \
               "Error: Wrong number of event data keys."
        assert mySiteConfig.eventData["catalog"] == "trivialcatalog_file://gpfs1/grid/grid-app/cmssoft/cms/SITECONF/local/PhEDEx/storage.xml?protocol=direct", \
               "Eroror: Event data catalog is wrong."

        goldenServers = ["http://cmsfrontier.cern.ch:8000/FrontierInt",
                         "http://cmsfrontier1.cern.ch:8000/FrontierInt",
                         "http://cmsfrontier2.cern.ch:8000/FrontierInt",
                         "http://cmsfrontier3.cern.ch:8000/FrontierInt"]
                         
        for frontierServer in mySiteConfig.frontierServers:
            assert frontierServer in goldenServers, \
                   "Error: Unknown server: %s" % frontierServer
            goldenServers.remove(frontierServer)

        assert len(goldenServers) == 0, \
               "Error: Missing frontier servers."

        goldenProxies = ["http://se1.accre.vanderbilt.edu:3128"]
        
        for frontierProxy in mySiteConfig.frontierProxies:
            assert frontierProxy in goldenProxies, \
                   "Error: Unknown proxy: %s" % frontierProxy
            goldenProxies.remove(frontierProxy)

        assert len(goldenProxies) == 0, \
                "Error: Missing proxy servers."

        assert mySiteConfig.localStageOut["se-name"] == "se1.accre.vanderbilt.edu", \
               "Error: Wrong se name from local stageout."
        assert mySiteConfig.localStageOut["command"] == "srmv2", \
               "Error: Wrong stage out command."
        assert mySiteConfig.localStageOut["catalog"] == "trivialcatalog_file://gpfs1/grid/grid-app/cmssoft/cms/SITECONF/local/PhEDEx/storage.xml?protocol=srmv2", \
               "Error: TFC catalog is not correct."

        assert len(mySiteConfig.fallbackStageOut) == 1, \
               "Error: Incorrect number of fallback stageout methods"
        assert mySiteConfig.fallbackStageOut[0]["command"] == "srmv2-lcg", \
               "Error: Incorrect fallback command."
        assert mySiteConfig.fallbackStageOut[0]["se-name"] == "se1.accre.vanderbilt.edu", \
               "Error: Incorrect fallback SE."
        assert mySiteConfig.fallbackStageOut[0]["lfn-prefix"] == "srm://se1.accre.vanderbilt.edu:6288/srm/v2/server?SFN=", \
               "Error: Incorrect fallback LFN prefix."
        return

if __name__ == "__main__":
    unittest.main()  
