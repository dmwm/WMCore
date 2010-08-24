#!/usr/bin/env python
"""
_TrivialFileCatalog_t_

Test the parsing of the TFC.
"""




import os
import unittest
from xml.dom.minidom import parseString
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
    
    def testRoundTrip(self):
        """
        Round trip tests failed
        """
        tfc_file = os.path.join(getWMBASE(),
                                   "test/python/WMCore_t/Storage_t",
                                   "T1_US_FNAL_TrivialFileCatalog.xml")
        tfc = readTFC(tfc_file)
        
        # Check that an lfn goes to an srmv2 pfn and comes back as the same lfn
        in_lfn = '/store/data/my/data'
        
        in_pfn = tfc.matchLFN('srmv2', in_lfn)
        out_lfn = tfc.matchPFN('srmv2', in_pfn)
        self.assertEqual(in_lfn, out_lfn)
        
        out_pfn = tfc.matchLFN('srmv2', out_lfn)
        self.assertEqual(in_pfn, out_pfn)
        
    def testDataServiceXML(self):
        phedex = PhEDEx(responseType='xml')
        
        site = 'T2_UK_SGrid_Bristol'
        lfn = '/store/users/metson/file'
        protocol = 'srmv2'
        phedex.getNodeTFC(site)
        
        tfc_file = phedex.cacheFileName('tfc', inputdata={'node': site})
        tfc = readTFC(tfc_file)
        print tfc
        # Hacky XML parser 
        phedex_dom = parseString(phedex.getPFN(site, lfn, protocol))
         
        phedex_pfn = phedex_dom.getElementsByTagName("mapping")[0].getAttribute('pfn') 
        
        pfn = tfc.matchLFN(protocol, lfn)
        msg = 'TFC pfn (%s) did not match PhEDEx pfn (%s)' % (pfn, phedex_pfn)
        assert phedex_pfn == pfn,  msg
    
if __name__ == "__main__":
    unittest.main()

