#!/usr/bin/python
"""

General test for SiteConfigs and TrivialFileCatalogs

"""

__revision__ = "$Id: SiteConfigTrivialFileCatalog_t.py,v 1.1 2009/11/19 21:18:34 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import os
import os.path
import unittest

from WMQuality.TestInit import TestInit
from WMCore.Storage.TrivialFileCatalog import tfcFilename, tfcProtocol, readTFC, TrivialFileCatalog
from WMCore.Storage.SiteLocalConfig    import SiteLocalConfig

class SiteConfigTrivialFileCatalogTest(unittest.TestCase):
    """
    
    General test for SiteConfigs and TrivialFileCatalogs
    
    """

    def setUp(self):
        """
        Very little to do.
        """

        return


    def tearDown(self):
        """
        Even less to do.
        """


        return


    def testA_TrivialFileCatalog(self):
        """
        Run some simple tests on reading a trivialFileCatalog

        """

        tfcFilename = os.path.join(os.getcwd(), "T1_US_TFC.xml")

        if not os.path.exists(tfcFilename):
            raise Exception("No TrivialFileCatalog found!")


        tfcInstance = readTFC(tfcFilename)

        self.assertEqual(type(tfcInstance), type(TrivialFileCatalog()))

        #Look for similarities in each node of the TFC file
        for x in tfcInstance:
            self.assertEqual(x.has_key('path-match-expr'), True)
            self.assertEqual(x.has_key('path-match'), True)
            self.assertEqual(x.has_key('protocol'), True)
            self.assertEqual(x.has_key('result'), True)
            self.assertEqual(x.has_key('chain'), True)
            self.assertEqual(x['protocol'] in ['direct', 'dcap', 'srm', 'srmv2'], True, 'Could not find protocol %s' % (x['protocol']))
            self.assertEqual(x['chain'], None, 'Invalid chain %s' % (x['chain']))


        return


    def testB_SiteLocalConfig(self):
        """
        Run some simple tests on reading a siteLocalConfig

        """

        slcFilename = os.path.join(os.getcwd(), "T1_US_SiteLocalConfig.xml")

        if not os.path.isfile(slcFilename):
            raise Exception("No SiteLocalConfig found!")

        #Actually read the slc file
        slcInstance = SiteLocalConfig(slcFilename)


        #Look for basic file features
        self.assertEqual(slcInstance.eventData['catalog'], \
                         'trivialcatalog_file:/uscmst1/prod/sw/cms/SITECONF/T1_US_FNAL/PhEDEx/storage.xml?protocol=dcap')
        self.assertEqual(slcInstance.siteName, 'T1_US_FNAL')
        self.assertEqual(slcInstance.fallbackStageOut, [])
        self.assertEqual(slcInstance.calibData, {'frontier-connect': 'None'})

        return

if __name__ == "__main__":
    unittest.main()  
