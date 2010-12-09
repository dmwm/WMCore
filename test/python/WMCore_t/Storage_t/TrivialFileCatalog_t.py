#!/usr/bin/env python
"""
_TrivialFileCatalog_t_

Test the parsing of the TFC.
"""

import os
import unittest
import nose
import tempfile

from xml.dom.minidom import parseString
from WMCore.WMInit import getWMBASE

from WMQuality.TestInit import TestInit

from WMCore.Storage.TrivialFileCatalog import tfcFilename, tfcProtocol, readTFC, TrivialFileCatalog

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx


class TrivialFileCatalogTest(unittest.TestCase):
    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testTrivialFileCatalog(self):
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

        # Look for similarities in each node of the TFC file
        for mapping in ['lfn-to-pfn', 'pfn-to-lfn']:
            for x in tfcInstance[mapping]:
                self.assertEqual(x.has_key('path-match-expr'), True)
                self.assertEqual(x.has_key('path-match'), True)
                self.assertEqual(x.has_key('protocol'), True)
                self.assertEqual(x.has_key('result'), True)
                self.assertEqual(x.has_key('chain'), True)
                self.assertEqual(x['protocol'] in ['direct', 'dcap', 'srm', 'srmv2'],
                                 True, 'Could not find protocol %s' % (x['protocol']))
                self.assertEqual(x['chain'], None, 'Invalid chain %s' % (x['chain']))
    
    
    def testRoundTrip(self):
        """
        Test PFN, LFN matching upon an existing TFC XML file
        
        """
        tfc_file = os.path.join(getWMBASE(),
                                "test/python/WMCore_t/Storage_t",
                                "T1_US_FNAL_TrivialFileCatalog.xml")
        tfc = readTFC(tfc_file)
        
        # Check that an lfn goes to an srmv2 pfn and comes back as the same lfn
        in_lfn = '/store/data/my/data'        
        in_pfn = "srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/store/data/my/data" 
        out_pfn = tfc.matchLFN('srmv2', in_lfn)
        self.assertEqual(out_pfn, in_pfn)
        out_lfn = tfc.matchPFN('srmv2', in_pfn)
        self.assertEqual(in_lfn, out_lfn)
        out_pfn = tfc.matchLFN('srmv2', out_lfn)
        self.assertEqual(in_pfn, out_pfn)
        
        
    def testRoundTripWithChain(self):
        """
        Test PFN, LFN conversion when rules are chained.
        
        """
        tfc = TrivialFileCatalog()
        
        match = "/+(.*)"
        result = "/castor/cern.ch/cms/$1"
        tfc.addMapping("direct", match, result, mapping_type = "lfn-to-pfn")
        
        match = "(.*)"
        result = "$1"
        tfc.addMapping("stageout", match, result, chain = "direct",
                       mapping_type = "lfn-to-pfn")
                               
        in_lfn = "/T0/hufnagel/store/t0temp/WMAgentTier0Commissioning/A/RECO/v1dh/0000/4CF196F9-E302-E011-8517-0030487CD716.root"
        in_pfn = "/castor/cern.ch/cms" + in_lfn

        out_pfn = tfc.matchLFN("stageout", in_lfn)
        self.assertNotEqual(out_pfn, in_lfn)
        self.assertEqual(out_pfn, in_pfn)

        tfc = TrivialFileCatalog()
        
        match = "/+castor/cern\.ch/cms/(.*)"
        result = "/$1"
        tfc.addMapping("direct", match, result, mapping_type = "pfn-to-lfn")

        match = "(.*)"
        result = "$1"
        tfc.addMapping("stageout", match, result, chain = "direct",
                       mapping_type = "pfn-to-lfn")
        
        out_lfn = tfc.matchPFN("stageout", in_pfn)
        self.assertEqual(out_lfn, in_lfn)
                
        
    def testDataServiceXML(self):
        # asks for PEM pass phrase ...
        raise nose.SkipTest
        phedex = PhEDEx(responseType='xml')
        
        site = 'T2_UK_SGrid_Bristol'
        lfn = '/store/users/metson/file'
        protocol = 'srmv2'
        phedex.getNodeTFC(site)
        
        tfc_file = phedex.cacheFileName('tfc', inputdata={'node': site})
        tfc = readTFC(tfc_file)

        pfn_dict = phedex.getPFN(site, lfn, protocol)
        phedex_pfn = pfn_dict[(site, lfn)]
        pfn = tfc.matchLFN(protocol, lfn)
        msg = 'TFC pfn (%s) did not match PhEDEx pfn (%s)' % (pfn, phedex_pfn)
        self.assertEqual(phedex_pfn, pfn, msg)
        
        
    def testAddMapping(self):
        tfc = TrivialFileCatalog()
        lfn = "some_lfn"
        pfn = "some_pfn"
        tfc.addMapping("direct", lfn, pfn, mapping_type = 'lfn-to-pfn')
        tfc.addMapping("direct", pfn, lfn, mapping_type = 'pfn-to-lfn')
        out_pfn = tfc.matchLFN("direct", lfn)
        out_lfn = tfc.matchPFN("direct", pfn)
        self.assertEqual(lfn, out_lfn, "Error: incorrect matching")
        self.assertEqual(pfn, out_pfn, "Error: incorrect matching")
                
    
    def testSaveTFC(self):
        tfc = TrivialFileCatalog()
        lfn = "some_lfn"
        pfn = "some_pfn"
        tfc.addMapping("direct", lfn, pfn, mapping_type = 'lfn-to-pfn')
        tfc.addMapping("direct", pfn, lfn, mapping_type = 'pfn-to-lfn')

        f = tempfile.NamedTemporaryFile("w+", delete = True) # read / write
        tfcStr = str(tfc.getXML())
        f.write(tfcStr)
        f.flush()
        f.seek(0)
        
        tfcInstance = readTFC(f.name)
        out_pfn = tfc.matchLFN("direct", lfn)
        out_lfn = tfc.matchPFN("direct", pfn)
        self.assertEqual(lfn, out_lfn, "Error: incorrect matching")
        self.assertEqual(pfn, out_pfn, "Error: incorrect matching")
        f.close()
        
        
if __name__ == "__main__":
    unittest.main()