#!/usr/bin/env python
"""
_DBSReader_t_

Unit test for the DBS helper class.
"""

import unittest
from nose.plugins.attrib import attr

from WMCore.Services.DBS.DBSReader import DBSReader as DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError

# a small dataset that should always exist
DATASET = '/HighPileUp/Run2011A-v1/RAW'
BLOCK = '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace'
FILE = '/store/data/Run2011A/HighPileUp/RAW/v1/000/173/657/B293AF24-BFCB-E011-8F85-BCAEC5329701.root'


class DBSReaderTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Initialize the PhEDEx API to point at the test server.
        """
        #self.endpoint = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        self.endpoint = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        self.dbs = None
        return

    @attr("integration")
    def testListDatatiers(self):
        """
        listDatatiers returns all datatiers available
        """
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.listDatatiers()
        self.assertTrue('RAW' in results)
        self.assertTrue('GEN-SIM-RECO' in results)
        self.assertTrue('GEN-SIM' in results)
        self.assertFalse('RAW-ALAN' in results)
        return

    @attr("integration")
    def testListPrimaryDatasets(self):
        """
        listPrimaryDatasets returns known primary datasets
        """
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.listPrimaryDatasets('Jet*')
        self.assertTrue('Jet' in results)
        self.assertTrue('JetMET' in results)
        self.assertTrue('JetMETTau' in results)
        self.assertFalse(self.dbs.listPrimaryDatasets('DoesntExist'))
        return

    @attr("integration")
    def testMatchProcessedDatasets(self):
        """
        matchProcessedDatasets returns known processed datasets
        """
        self.dbs = DBSReader(self.endpoint)
        dataset = self.dbs.matchProcessedDatasets('Jet', 'RAW', 'Run2011A-v1')
        self.assertEqual(1, len(dataset))
        self.assertEqual(['/Jet/Run2011A-v1/RAW'], dataset[0]['PathList'])
        self.assertEqual('Run2011A-v1', dataset[0]['Name'])
        self.assertFalse(self.dbs.matchProcessedDatasets('Jet', 'RAW', 'Run2011A-v666'))

    def testlistRuns(self):
        """listRuns returns known runs"""
        self.dbs = DBSReader(self.endpoint)
        runs = self.dbs.listRuns(dataset=DATASET)
        self.assertEqual(46, len(runs))
        self.assertTrue(174074 in runs)
        runs = self.dbs.listRuns(block=BLOCK)
        self.assertEqual(1, len(runs))
        self.assertEqual([173657], runs)

    def testlistRunLumis(self):
        """listRunLumis returns known runs and lumicounts (None for DBS3)"""
        self.dbs = DBSReader(self.endpoint)
        runs = self.dbs.listRunLumis(dataset=DATASET)
        self.assertEqual(46, len(runs))
        self.assertTrue(173692 in runs)
        self.assertEqual(runs[173692], None)
        runs = self.dbs.listRunLumis(block=BLOCK)
        self.assertEqual(1, len(runs))
        self.assertTrue(173657 in runs)
        self.assertEqual(runs[173657], None)

    @attr("integration")
    def testListProcessedDatasets(self):
        """listProcessedDatasets returns known processed datasets"""
        self.dbs = DBSReader(self.endpoint)
        datasets = self.dbs.listProcessedDatasets('Jet', 'RAW')
        self.assertTrue('Run2011A-v1' in datasets)
        self.assertTrue('Run2011B-v1' in datasets)
        self.assertFalse(self.dbs.listProcessedDatasets('Jet', 'blah'))
        self.assertFalse(self.dbs.listProcessedDatasets('blah', 'RAW'))

    def testlistDatasetFiles(self):
        """listDatasetFiles returns files in dataset"""
        self.dbs = DBSReader(self.endpoint)
        files = self.dbs.listDatasetFiles(DATASET)
        self.assertEqual(49, len(files))
        self.assertTrue(FILE in files)

    def testlistDatasetFileDetails(self):
        """testlistDatasetFilesDetails returns lumis, events, and parents of a dataset"""
        TESTFILE = '/store/data/Run2011A/HighPileUp/RAW/v1/000/173/658/56484BAB-CBCB-E011-AF00-BCAEC518FF56.root'
        self.dbs = DBSReader(self.endpoint)
        details = self.dbs.listDatasetFileDetails(DATASET)
        self.assertEqual(len(details), 49)
        self.assertTrue(TESTFILE in details)
        self.assertEqual(details[TESTFILE]['NumberOfEvents'], 545)
        self.assertEqual(details[TESTFILE]['file_size'], 286021145)
        self.assertEqual(details[TESTFILE]['BlockName'], '/HighPileUp/Run2011A-v1/RAW#dd6e0796-cbcc-11e0-80a9-003048caaace')
        self.assertEqual(details[TESTFILE]['Md5'], 'NOTSET')
        self.assertEqual(details[TESTFILE]['md5'], 'NOTSET')
        self.assertEqual(details[TESTFILE]['Adler32'], 'a41a1446')
        self.assertEqual(details[TESTFILE]['adler32'], 'a41a1446')
        self.assertEqual(details[TESTFILE]['Checksum'], '22218315')
        self.assertEqual(details[TESTFILE]['check_sum'], '22218315')
        self.assertTrue(173658 in details[TESTFILE]['Lumis'])
        self.assertEqual(sorted(details[TESTFILE]['Lumis'][173658]),
                         [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                          27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
                          51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
                          75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98,
                          99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111])

    def testGetDBSSummaryInfo(self):
        """getDBSSummaryInfo returns summary of dataset and block"""
        self.dbs = DBSReader(self.endpoint)
        dataset = self.dbs.getDBSSummaryInfo(DATASET)
        self.assertEqual(dataset['path'], DATASET)
        self.assertEqual(dataset['block'], '')
        self.assertEqual(dataset['NumberOfEvents'], 22075)
        self.assertEqual(dataset['NumberOfBlocks'], 46)
        self.assertEqual(dataset['FileSize'], 4001680824)
        self.assertEqual(dataset['file_size'], 4001680824)
        self.assertEqual(dataset['NumberOfFiles'], 49)
        self.assertEqual(dataset['NumberOfLumis'], 7223)

        block = self.dbs.getDBSSummaryInfo(DATASET, BLOCK)
        self.assertEqual(block['path'], '')
        self.assertEqual(block['block'], BLOCK)
        self.assertEqual(block['NumberOfEvents'], 377)
        self.assertEqual(block['NumberOfBlocks'], 1)
        self.assertEqual(block['FileSize'], 150780132)
        self.assertEqual(block['file_size'], 150780132)
        self.assertEqual(block['NumberOfFiles'], 2)
        self.assertEqual(block['NumberOfLumis'], 94)

        self.assertRaises(DBSReaderError, self.dbs.getDBSSummaryInfo, DATASET + 'blah')
        self.assertRaises(DBSReaderError, self.dbs.getDBSSummaryInfo, DATASET, BLOCK + 'asas')

    @attr("integration")
    def testGetFileBlocksInfo(self):
        """getFileBlocksInfo returns block info, including location lookup"""
        self.dbs = DBSReader(self.endpoint)
        blocks = self.dbs.getFileBlocksInfo(DATASET)
        block = self.dbs.getFileBlocksInfo(DATASET, blockName=BLOCK)
        self.assertEqual(1, len(block))
        block = block[0]
        self.assertEqual(46, len(blocks))
        self.assertTrue(block['Name'] in [x['Name'] for x in blocks])
        self.assertEqual(BLOCK, block['Name'])
        self.assertEqual(0, block['OpenForWriting'])
        self.assertEqual(150780132, block['BlockSize'])
        self.assertEqual(2, block['NumberOfFiles'])
        # possibly fragile but assume block located at least at cern
        sites = [x['Name'] for x in block['StorageElementList'] if x['Name'].find('cern.ch') > -1]
        self.assertTrue(sites)

        # weird error handling - depends on whether block or dataset is missing
        self.assertRaises(DBSReaderError, self.dbs.getFileBlocksInfo, DATASET + 'blah')
        self.assertRaises(DBSReaderError, self.dbs.getFileBlocksInfo, DATASET, blockName=BLOCK + 'asas')

    def testListFileBlocks(self):
        """listFileBlocks returns block names in dataset"""
        self.dbs = DBSReader(self.endpoint)
        blocks = self.dbs.listFileBlocks(DATASET)
        self.assertTrue(BLOCK in blocks)
        # block is closed
        block = self.dbs.listFileBlocks(DATASET, blockName=BLOCK, onlyClosedBlocks=True)[0]
        self.assertEqual(block, BLOCK)
        self.assertTrue(BLOCK in block)

    def testListOpenFileBlocks(self):
        """listOpenFileBlocks finds open blocks"""
        # hard to find a dataset with open blocks, so don't bother
        self.dbs = DBSReader(self.endpoint)
        self.assertFalse(self.dbs.listOpenFileBlocks(DATASET))

    def testBlockExists(self):
        """blockExists returns existence of blocks"""
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(self.dbs.blockExists(BLOCK))
        self.assertRaises(DBSReaderError, self.dbs.blockExists, DATASET + '#somethingelse')

    def testListFilesInBlock(self):
        """listFilesInBlock returns files in block"""
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(FILE in [x['LogicalFileName'] for x in self.dbs.listFilesInBlock(BLOCK)])
        self.assertRaises(DBSReaderError, self.dbs.listFilesInBlock, DATASET + '#blah')

    def testListFilesInBlockWithParents(self):
        """listFilesInBlockWithParents gets files with parents for a block"""
        self.dbs = DBSReader(self.endpoint)
        files = self.dbs.listFilesInBlockWithParents('/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0')
        self.assertEqual(4, len(files))
        self.assertEqual('/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0', files[0]['block_name'])
        self.assertEqual('/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0', files[0]['BlockName'])
        self.assertEqual('/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/1043E89F-2DCF-E411-9CAE-02163E013751.root',
                         files[0]['ParentList'][0]['LogicalFileName'])

        self.assertRaises(DBSReaderError, self.dbs.listFilesInBlockWithParents, BLOCK + 'asas')

    def testLfnsInBlock(self):
        """lfnsInBlock returns lfns in block"""
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(FILE in [x['logical_file_name'] for x in self.dbs.lfnsInBlock(BLOCK)])
        self.assertRaises(DBSReaderError, self.dbs.lfnsInBlock, BLOCK + 'asas')

    @attr("integration")
    def testListFileBlockLocation(self):
        """listFileBlockLocation returns block location"""
        WRONG_BLOCK = BLOCK[:-4]+'abcd'
        BLOCK2 = '/HighPileUp/Run2011A-v1/RAW#6021175e-cbfb-11e0-80a9-003048caaace'
        DBS_BLOCK = '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-'+\
                                    'ea0972193530f531086947d06eb0f121/USER#fb978442-a61b-413a-b4f4-526e6cdb142e'
        DBS_BLOCK2 = '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-'+\
                                    'ea0972193530f531086947d06eb0f121/USER#0b04d417-d734-4ef2-88b0-392c48254dab'
        self.dbs = DBSReader('https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader/')
        # assume one site is cern
        sites = [x for x in self.dbs.listFileBlockLocation(BLOCK) if x and x.find('cern.ch') > -1]
        self.assertTrue(sites)
        #This block is only found on DBS
        self.assertTrue(self.dbs.listFileBlockLocation(DBS_BLOCK))
        # doesn't raise on non-existant block
        self.assertFalse(self.dbs.listFileBlockLocation(WRONG_BLOCK))
        #test bulk call:
        ## two blocks in phedex
        self.assertEqual(2, len(self.dbs.listFileBlockLocation([BLOCK, BLOCK2])))
        ## one block in phedex one does not exist
        self.assertEqual(1, len(self.dbs.listFileBlockLocation([BLOCK, WRONG_BLOCK])))
        ## one in phedex one in dbs
        self.assertEqual(2, len(self.dbs.listFileBlockLocation([BLOCK, DBS_BLOCK])))
        ## two in dbs
        self.assertEqual(2, len(self.dbs.listFileBlockLocation([DBS_BLOCK, DBS_BLOCK2])))
        ## one in DBS and one does not exist
        self.assertEqual(1, len(self.dbs.listFileBlockLocation([DBS_BLOCK, WRONG_BLOCK])))

    def testGetFileBlock(self):
        """getFileBlock returns block"""
        self.dbs = DBSReader(self.endpoint)
        block = self.dbs.getFileBlock(BLOCK)
        self.assertEqual(len(block), 1)
        block = block[BLOCK]
        self.assertEqual(2, len(block['Files']))

        self.assertRaises(DBSReaderError, self.dbs.getFileBlock, BLOCK + 'asas')

    def testGetFileBlockWithParents(self):
        """getFileBlockWithParents returns block and parents"""
        self.dbs = DBSReader(self.endpoint)
        block = self.dbs.getFileBlockWithParents('/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0')
        self.assertEqual(len(block), 1)
        block = block['/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0']
        self.assertEqual('/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/1043E89F-2DCF-E411-9CAE-02163E013751.root',
                         block['Files'][0]['ParentList'][0]['LogicalFileName'])

        self.assertRaises(DBSReaderError, self.dbs.getFileBlockWithParents, BLOCK + 'asas')

    def testGetFiles(self):
        """getFiles returns files in dataset"""
        self.dbs = DBSReader(self.endpoint)
        files = self.dbs.getFiles(DATASET)
        self.assertEqual(len(files), 46)

    def testListBlockParents(self):
        """listBlockParents returns block parents"""
        self.dbs = DBSReader(self.endpoint)
        parents = self.dbs.listBlockParents('/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0')
        self.assertEqual(1, len(parents))
        self.assertEqual('/Cosmics/Commissioning2015-v1/RAW#942d76fe-cf0e-11e4-afad-001e67ac06a0', parents[0]['Name'])
        sites = [x for x in parents[0]['StorageElementList'] if x.find("cern.ch") > -1]
        self.assertTrue(sites)

        self.assertFalse(self.dbs.listBlockParents('/Cosmics/Commissioning2015-v1/RAW#942d76fe-cf0e-11e4-afad-001e67ac06a0'))

    def testBlockIsOpen(self):
        """blockIsOpen checks if a block is open"""
        self.dbs = DBSReader(self.endpoint)
        self.assertFalse(self.dbs.blockIsOpen(BLOCK))

    def testBlockToDatasetPath(self):
        """blockToDatasetPath extracts path from block name"""
        self.dbs = DBSReader(self.endpoint)
        self.assertEqual(self.dbs.blockToDatasetPath(BLOCK), DATASET)
        self.assertRaises(DBSReaderError, self.dbs.blockToDatasetPath, BLOCK + 'asas')

if __name__ == '__main__':
    unittest.main()
