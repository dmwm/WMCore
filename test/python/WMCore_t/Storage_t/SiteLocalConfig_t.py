#!/usr/bin/env python
"""
_SiteLocalConfig_t_

Unit test for parsing the site local config file.
"""
from __future__ import print_function

import os
import unittest

from WMCore.WMBase import getTestBase

from WMCore.Storage.SiteLocalConfig import SiteLocalConfig, SiteConfigError
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

from nose.plugins.attrib import attr

class SiteLocalConfigTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFNALSiteLocalConfig(self):
        """
        _testFNALSiteLocalConfig_

        Verify that the FNAL site config file is parsed correctly.
        """
        os.environ['SITECONFIG_PATH'] = '/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL'
        fnalConfigFileName = os.path.join(getTestBase(),
                                          "WMCore_t/Storage_t",
                                          "T1_US_FNAL_SiteLocalConfig.xml")
        #fnalConfigFileName = '/cvmfs/cms.cern.ch/SITECONF/T1_DE_KIT/KIT-HOREKA/JobConfig/site-local-config.xml'
        #switch between old TFC and new Rucio data catalog by changing the last parameter, need to set SITECONFIG_PATH when this is False 
        #mySiteConfig = SiteLocalConfig(fnalConfigFileName,False) #set False to use new Rucio storage descriptions with storage.json
        mySiteConfig = SiteLocalConfig(fnalConfigFileName)
        #print(mySiteConfig.localStageOut)
        #print(mySiteConfig.fallbackStageOut)
        #tfcInstance = mySiteConfig.trivialFileCatalog()
        #print(tfcInstance)
        #for mapping in ['lfn-to-pfn', 'pfn-to-lfn']:
        #    for x in tfcInstance[mapping]:
        #      print(x)

        assert mySiteConfig.siteName == "T1_US_FNAL", "Error: Wrong site name."
        assert len(list(mySiteConfig.eventData)) == 1, "Error: Wrong number of event data keys."
        assert mySiteConfig.eventData["catalog"] == "trivialcatalog_file:/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL_Disk/PhEDEx/storage.xml?protocol=fallbackxrd", \
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
        
        assert mySiteConfig.stageOuts[0]["command"] == "xrdcp", \
                "Error: Wrong stage out command."
        assert mySiteConfig.stageOuts[0]["protocol"] == "XRootD",\
                "Error: Protocol is not correct."
        assert mySiteConfig.stageOuts[0]["option"] == "-p",\
                "Error: option is not correct."
        '''
        if mySiteConfig.useTFC:
            assert mySiteConfig.localStageOut["command"] == "stageout-xrdcp-fnal", \
                "Error: Wrong stage out command."
            assert mySiteConfig.localStageOut["catalog"] == "trivialcatalog_file:/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL_Disk/PhEDEx/storage.xml?protocol=writexrd", \
                "Error: TFC catalog is not correct."
            assert mySiteConfig.fallbackStageOut == [], \
                "Error: Fallback config is incorrect."
        else:
            assert mySiteConfig.stageOuts[0]["command"] == "xrdcp", \
                "Error: Wrong stage out command."
            assert mySiteConfig.stageOuts[0]["protocol"] == "XRootD",\
                "Error: Protocol is not correct."
            assert mySiteConfig.stageOuts[0]["option"] == "-p",\
                "Error: option is not correct."
        '''          
        #assert False
        return

    def testVanderbiltSiteLocalConfig(self):
        """
        _testFNALSiteLocalConfig_

        Verify that the FNAL site config file is parsed correctly.
        """
        vandyConfigFileName = os.path.join(getTestBase(),
                                           "WMCore_t/Storage_t",
                                           "T3_US_Vanderbilt_SiteLocalConfig.xml")
        #still using legacy trivial catalog
        mySiteConfig = SiteLocalConfig(vandyConfigFileName,True)

        assert mySiteConfig.siteName == "T3_US_Vanderbilt", \
               "Error: Wrong site name."

        assert len(list(mySiteConfig.eventData)) == 1, \
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

        assert mySiteConfig.localStageOut["command"] == "srmv2", \
               "Error: Wrong stage out command."
        assert mySiteConfig.localStageOut["catalog"] == "trivialcatalog_file://gpfs1/grid/grid-app/cmssoft/cms/SITECONF/local/PhEDEx/storage.xml?protocol=srmv2", \
               "Error: TFC catalog is not correct."

        assert len(mySiteConfig.fallbackStageOut) == 1, \
               "Error: Incorrect number of fallback stageout methods"
        assert mySiteConfig.fallbackStageOut[0]["command"] == "srmv2-lcg", \
               "Error: Incorrect fallback command."
        assert mySiteConfig.fallbackStageOut[0]["lfn-prefix"] == "srm://se1.accre.vanderbilt.edu:6288/srm/v2/server?SFN=", \
               "Error: Incorrect fallback LFN prefix."
        return


    def testLoadingConfigFromOverridenEnvVarriable(self):
        """
        test SiteLocalConfig module method loadSiteLocalConfig when loading
        site config from location defined by WMAGENT_SITE_CONFIG_OVERRIDE
        env. variable

        """
        vandyConfigFileName = os.path.join(getTestBase(),
                                           "WMCore_t/Storage_t",
                                           "T3_US_Vanderbilt_SiteLocalConfig.xml")
        os.environ["WMAGENT_SITE_CONFIG_OVERRIDE"] = vandyConfigFileName

        #still using legacy trivial catalog
        mySiteConfig = loadSiteLocalConfig(True)
        self.assertEqual(mySiteConfig.siteName, "T3_US_Vanderbilt",
                         "Error: Wrong site name.")

    # this test requires access to CVMFS
    @attr("integration")
    def testSlcPhedexNodesEqualPhedexApiNodes(self):
        """
        For each site, verify that the stageout node specified in
        site-local-config.xml is the same as the one returned by the PhEDEx api.
        """
        os.environ["CMS_PATH"] = "/cvmfs/cms.cern.ch"
        os.environ["SITECONFIG_PATH"] = "/cvmfs/cms.cern.ch/SITECONF/local"

        nodes = ['FIXME']

        for d in os.listdir("/cvmfs/cms.cern.ch/SITECONF/"):
            # Only T0_, T1_... folders are needed
            if d[0] == "T":
                os.environ['WMAGENT_SITE_CONFIG_OVERRIDE'] ='/cvmfs/cms.cern.ch/SITECONF/%s/JobConfig/site-local-config.xml' % (d)
                try:
                    #still using legacy trivial catalog
                    slc = loadSiteLocalConfig(True)
                except SiteConfigError as e:
                    print(e.args[0])
                phedexNode = slc.localStageOut.get("phedex-node")
                self.assertTrue(phedexNode in nodes,
                                "Error: Node specified in SLC (%s) not in list returned by PhEDEx api" % phedexNode)
        return

if __name__ == "__main__":
    unittest.main()
