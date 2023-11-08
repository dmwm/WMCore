#!/usr/bin/env python
"""
_TrivialFileCatalog_t_

Test the parsing of the TFC.
"""

from builtins import str

import os
import unittest
import tempfile

from WMCore.WMBase import getTestBase

from WMCore.Storage.TrivialFileCatalog import readTFC, TrivialFileCatalog


class TrivialFileCatalogTest(unittest.TestCase):
    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testTrivialFileCatalog(self):
        """
        Run some simple tests on reading a trivialFileCatalog

        """
        tfcFilename = os.path.join(getTestBase(),
                                   "WMCore_t/Storage_t",
                                   "T1_US_FNAL_TrivialFileCatalog.xml")

        if not os.path.exists(tfcFilename):
            raise Exception("No TrivialFileCatalog found!")


        tfcInstance = readTFC(tfcFilename)

        self.assertEqual(type(tfcInstance), type(TrivialFileCatalog()))

        # Look for similarities in each node of the TFC file
        for mapping in ['lfn-to-pfn', 'pfn-to-lfn']:
            for x in tfcInstance[mapping]:
                self.assertEqual('path-match-expr' in x, True)
                self.assertEqual('path-match' in x, True)
                self.assertEqual('protocol' in x, True)
                self.assertEqual('result' in x, True)
                self.assertEqual('chain' in x, True)
                self.assertEqual(x['protocol'] in ['direct', 'dcap', 'srm', 'srmv2'],
                                 True, 'Could not find protocol %s' % (x['protocol']))
                self.assertEqual(x['chain'], None, 'Invalid chain %s' % (x['chain']))


    def testRoundTrip(self):
        """
        Test PFN, LFN matching upon an existing TFC XML file

        """
        tfc_file = os.path.join(getTestBase(),
                                "WMCore_t/Storage_t",
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

        in_lfn = "/this/is/a/test/lfn"
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

    def testMultipleRegexMatch(self):
        # Check that an lfn is converted to the right pfn
        in_lfn = '/store/user/fred/data'

        tfc_file = os.path.join(getTestBase(),
                                "WMCore_t/Storage_t",
                                "T2_CH_CERNBOX_TrivialFileCatalog.xml")
        tfc = readTFC(tfc_file)
        out_pfn = "root://eosuser.cern.ch/eos/user/f/fred/data"
        pfn = tfc.matchLFN('srmv2', in_lfn)
        self.assertEqual(out_pfn, pfn)

        tfc_file = os.path.join(getTestBase(),
                                "WMCore_t/Storage_t",
                                "T2_PT_NCG_Lisbon_TrivialFileCatalog.xml")
        tfc = readTFC(tfc_file)
        out_pfn = "srm://srm01.ncg.ingrid.pt:8444/srm/managerv2?SFN=/cmst3/store/user/fred/data"
        pfn = tfc.matchLFN('srmv2', in_lfn)
        self.assertEqual(out_pfn, pfn)

        tfc_file = os.path.join(getTestBase(),
                                "WMCore_t/Storage_t",
                                "T2_US_Florida_TrivialFileCatalog.xml")
        tfc = readTFC(tfc_file)
        out_pfn = "srm://srm.ihepa.ufl.edu:8443/srm/v2/server?SFN=/cms/data/store/user/fred/data"
        pfn = tfc.matchLFN('srmv2', in_lfn)
        self.assertEqual(out_pfn, pfn)

        tfc_file = os.path.join(getTestBase(),
                                "WMCore_t/Storage_t",
                                "T2_ES_IFCA_TrivialFileCatalog.xml")
        tfc = readTFC(tfc_file)
        out_pfn = "srm://srm01.ifca.es:8444/srm/managerv2?SFN=/cms/store/user/fred/data"
        pfn = tfc.matchLFN('srmv2', in_lfn)
        self.assertEqual(out_pfn, pfn)

        tfc_file = os.path.join(getTestBase(),
                                "WMCore_t/Storage_t",
                                "T2_US_Nebraska_TrivialFileCatalog.xml")
        tfc = readTFC(tfc_file)
        out_pfn = "srm://dcache07.unl.edu:8443/srm/v2/server?SFN=/mnt/hadoop/user/uscms01/pnfs/unl.edu/data4/cms/store/user/fred/data"
        pfn = tfc.matchLFN('srmv2', in_lfn)
        self.assertEqual(out_pfn, pfn)


if __name__ == "__main__":
    unittest.main()
