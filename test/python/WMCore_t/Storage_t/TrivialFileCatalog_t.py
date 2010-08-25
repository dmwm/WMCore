#!/usr/bin/env python
"""
_TrivialFileCatalog_t_

Test the parsing of the TFC.
"""

__revision__ = "$Id: TrivialFileCatalog_t.py,v 1.3 2010/07/12 16:06:56 metson Exp $"
__version__ = "$Revision: 1.3 $"

import os
import unittest

from WMCore.WMInit import getWMBASE

from WMQuality.TestInit import TestInit
from WMQuality.TestInit import requiresPython26

from WMCore.Storage.TrivialFileCatalog import tfcFilename, tfcProtocol, readTFC, TrivialFileCatalog

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

class TrivialFileCatalogTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @requiresPython26
    def testA_TrivialFileCatalog(self):
        """
        Run some simple tests on reading a trivialFileCatalog

        """

        tfcFilename = os.path.join(getWMBASE(),
                                   "test/python/WMCore_t/Storage_t",
                                   "T1_US_FNAL_TrivialFileCatalog.xml")

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

    def testDataServiceXML(self):
        phedex = PhEDEx(responseType='xml')
        
        site = 'T2_UK_SGrid_Bristol'
        
        phedex.getNodeTFC(site)
        
        tfc_file = phedex.cacheFileName('tfc', inputdata={'node': site})
        tfc = readTFC(tfc_file)
        
        pfn = tfc.matchLFN('srmv2', '/store/user/metson/file')
        
        assert pfn.startswith('srm')
    
if __name__ == "__main__":
    unittest.main()

