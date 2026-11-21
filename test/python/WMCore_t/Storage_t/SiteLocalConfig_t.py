#!/usr/bin/env python
"""
_SiteLocalConfig_t_

Unit test for parsing the site local config file.
"""
from __future__ import print_function

import os
import unittest

from nose.plugins.attrib import attr

from WMCore.Storage.SiteLocalConfig import SiteLocalConfig, SiteConfigError
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig
from WMCore.WMBase import getTestBase


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
        
        mySiteConfig = SiteLocalConfig(fnalConfigFileName)

        assert mySiteConfig.siteName == "T1_US_FNAL", "Error: Wrong site name."
        assert len(list(mySiteConfig.eventData)) == 1, "Error: Wrong number of event data keys."
        assert mySiteConfig.eventData[
                   "catalog"] == "trivialcatalog_file:/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL_Disk/PhEDEx/storage.xml?protocol=fallbackxrd", \
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
        
        #test second stage out method with command extraction from rules
        assert mySiteConfig.stageOuts[0]["command"] == "xrdcp", \
            "Error: Wrong stage out command."
        assert mySiteConfig.stageOuts[0]["protocol"] == "XRootD", \
            "Error: Protocol is not correct."
        assert mySiteConfig.stageOuts[0]["option"] == "-p", \
            "Error: option is not correct."
        
        #assert False
        return
    
    def testGetDefaultStageOutCmd(self):
        """
        _testGetDefaultStageOutCmd_

        Verify that the default stage out command is returned correctly for given protocol.
        """
        os.environ['SITECONFIG_PATH'] = os.path.join(getTestBase(),
                                                    'WMCore_t/Storage_t',
                                                    'T1_DE_KIT')
        configFileName = os.path.join(getTestBase(),
                                          "WMCore_t/Storage_t",
                                          "T1_DE_KIT/JobConfig/site-local-config-testStageOut-T1_DE_KIT.xml")
       
        mySiteConfig = SiteLocalConfig(configFileName)
        #test the first stage out method with command extraction from prefix
        assert mySiteConfig.stageOuts[0]["command"] == "gfal2", \
            "Error: Wrong stage out command."
        assert mySiteConfig.stageOuts[0]["protocol"] == "WebDAV", \
            "Error: Protocol is not correct."
        #test the second stage out method with command extraction from rules
        assert mySiteConfig.stageOuts[1]["command"] == "gfal2", \
            "Error: Wrong stage out command."
        assert mySiteConfig.stageOuts[1]["protocol"] == "WebDAV", \
            "Error: Protocol is not correct."
        #test the third stage out method with no command found, fall to default gfal2
        assert mySiteConfig.stageOuts[2]["command"] == "gfal2", \
            "Error: Wrong stage out command."
        assert mySiteConfig.stageOuts[2]["protocol"] == "xrootd-module", \
            "Error: Protocol is not correct."
        #test the fourth stage out method with "prefix": "root://172.26.19.197:1094//root://cmsxrootd-test.gridka.de:1094/"
        assert mySiteConfig.stageOuts[3]["command"] == "xrdcp", \
            "Error: Wrong stage out command."
        assert mySiteConfig.stageOuts[3]["protocol"] == "XRootDHoreKaGridKa", \
            "Error: Protocol is not correct."
        
        os.environ['SITECONFIG_PATH'] = '/cvmfs/cms.cern.ch/SITECONF/T1_IT_CNAF'
        configFileName = os.path.join(getTestBase(),
                                          "WMCore_t/Storage_t",
                                          "T1_IT_CNAF_SiteLocalConfig.xml")
       
        mySiteConfig = SiteLocalConfig(configFileName)
        #test the first stage out method for command 'cp' with protocol 'file'
        assert mySiteConfig.stageOuts[0]["command"] == "cp", \
            "Error: Wrong stage out command."
        assert mySiteConfig.stageOuts[0]["protocol"] == "file", \
            "Error: Protocol is not correct."

        #assert False
        return

    def testLoadingConfigFromOverridenEnvVarriable(self):
        """
        test SiteLocalConfig module method loadSiteLocalConfig when loading
        site config from location defined by WMAGENT_SITE_CONFIG_OVERRIDE
        env. variable

        """
        vandyConfigFileName = os.path.join(getTestBase(),
                                           "WMCore_t/Storage_t",
                                           "T1_US_FNAL_SiteLocalConfig.xml")
        os.environ["WMAGENT_SITE_CONFIG_OVERRIDE"] = vandyConfigFileName
        os.environ["SITECONFIG_PATH"] = "/cvmfs/cms.cern.ch/SITECONF/T1_US_FNAL"

        # still using legacy trivial catalog
        mySiteConfig = loadSiteLocalConfig()
        self.assertEqual(mySiteConfig.siteName, "T1_US_FNAL",
                         "Error: Wrong site name.")

    # this test requires access to CVMFS
    @attr("integration")
    def testSlcPhedexNodesEqualPhedexApiNodes(self):
        """
        For each site, verify that the stageout node specified in
        site-local-config.xml is the same as the one returned by the PhEDEx api.
        """
        os.environ["CMS_PATH"] = "/cvmfs/cms.cern.ch"

        nodes = ['FIXME']

        for d in os.listdir("/cvmfs/cms.cern.ch/SITECONF/"):
            # Only T0_, T1_... folders are needed
            if d[0] == "T":
                os.environ["SITECONFIG_PATH"] = "/cvmfs/cms.cern.ch/SITECONF/%s" % (d)
                os.environ[
                    'WMAGENT_SITE_CONFIG_OVERRIDE'] = '/cvmfs/cms.cern.ch/SITECONF/%s/JobConfig/site-local-config.xml' % (
                    d)
                try:
                    # still using legacy trivial catalog
                    slc = loadSiteLocalConfig()
                except SiteConfigError as e:
                    print(e.args[0])
                phedexNode = slc.localStageOut.get("phedex-node")
                self.assertTrue(phedexNode in nodes,
                                "Error: Node specified in SLC (%s) not in list returned by PhEDEx api" % phedexNode)
        return


if __name__ == "__main__":
    unittest.main()
