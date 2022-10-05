#!/usr/bin/env python
"""
_DBSReader_t_

Unit test for the DBS helper class.
"""

import unittest

from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from nose.plugins.attrib import attr

from Utils.PythonVersion import PY3

from WMCore.Services.DBS.DBS3Reader import getDataTiers, DBS3Reader as DBSReader
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

# A small dataset that should always exist
BAD_DATASET = '/HighPileUp/Run2011A-v1/RAW-BLAH'
DATASET = '/HighPileUp/Run2011A-v1/RAW'
BLOCK = '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace'
FILE = '/store/data/Run2011A/HighPileUp/RAW/v1/000/173/657/B293AF24-BFCB-E011-8F85-BCAEC5329701.root'
FILE2 = '/store/data/Run2011A/HighPileUp/RAW/v1/000/173/660/EE0B83F9-F3CB-E011-B996-BCAEC5329710.root'

# A RECO dataset that has parents (also small)
DATASET_WITH_PARENTS = '/Cosmics/ComissioningHI-PromptReco-v1/RECO'
BLOCK_WITH_PARENTS = DATASET_WITH_PARENTS + '#7020873e-0dcd-11e1-9b6c-003048caaace'
FILE1_WITH_PARENT = '/store/data/ComissioningHI/Cosmics/RECO/PromptReco-v1/000/180/841/368B76AA-4F09-E111-82CB-BCAEC5329721.root'
FILE2_WITH_PARENT = '/store/data/ComissioningHI/Cosmics/RECO/PromptReco-v1/000/180/852/D82D6996-1B0B-E111-AA40-003048CF94A6.root'

PARENT_DATASET = '/Cosmics/ComissioningHI-v1/RAW'
PARENT_BLOCK = PARENT_DATASET + '#929366bc-0c31-11e1-b764-003048caaace'
PARENT_FILE = '/store/data/ComissioningHI/Cosmics/RAW/v1/000/181/369/662EAD44-300C-E111-A709-BCAEC518FF62.root'


class DBSReaderTest(EmulatedUnitTestCase):
#class DBSReaderTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Initialize the PhEDEx API to point at the test server.
        """

        self.endpoint = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'
        self.dbs = None
        super(DBSReaderTest, self).setUp()
        if PY3:
            self.assertItemsEqual = self.assertCountEqual
        return

    def tearDown(self):
        """
        _tearDown_

        """

        super(DBSReaderTest, self).tearDown()
        return

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

    def testGetDataTiers(self):
        """
        Test the getDataTiers function
        """
        results = getDataTiers(self.endpoint)
        self.assertTrue('RAW' in results)
        self.assertTrue('GEN-SIM-RECO' in results)
        self.assertTrue('GEN-SIM' in results)
        self.assertFalse('RAW-ALAN' in results)
        # dbsUrl is mandatory
        with self.assertRaises(TypeError):
            _ = getDataTiers()
        return

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

    def testListProcessedDatasets(self):
        """listProcessedDatasets returns known processed datasets"""
        self.dbs = DBSReader(self.endpoint)
        datasets = self.dbs.listProcessedDatasets('Jet', 'RAW')
        self.assertTrue('Run2011A-v1' in datasets)
        self.assertTrue('Run2011B-v1' in datasets)
        self.assertFalse(self.dbs.listProcessedDatasets('blah', 'RAW'))
        # with the migration to Go DBS, it now raises an HTTPError exception like
        # Function:dbs.validator.Check Message:unable to match 'data_tier_name' value 'blah' Error: invalid parameter(s)"
        with self.assertRaises(HTTPError):
            self.dbs.listProcessedDatasets('Jet', 'blah')

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
        self.assertEqual(details[TESTFILE]['BlockName'],
                         '/HighPileUp/Run2011A-v1/RAW#dd6e0796-cbcc-11e0-80a9-003048caaace')
        self.assertEqual(details[TESTFILE]['Md5'], 'NOTSET')
        self.assertEqual(details[TESTFILE]['md5'], 'NOTSET')
        self.assertEqual(details[TESTFILE]['Adler32'], 'a41a1446')
        self.assertEqual(details[TESTFILE]['adler32'], 'a41a1446')
        self.assertEqual(details[TESTFILE]['Checksum'], '22218315')
        self.assertEqual(details[TESTFILE]['check_sum'], '22218315')
        self.assertTrue(173658 in details[TESTFILE]['Lumis'])
        self.assertEqual(sorted(details[TESTFILE]['Lumis'][173658]),
                         [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                          27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
                          50,
                          51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73,
                          74,
                          75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97,
                          98,
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
        self.assertEqual(block['path'], DATASET)
        self.assertEqual(block['block'], BLOCK)
        self.assertEqual(block['NumberOfEvents'], 377)
        self.assertEqual(block['NumberOfBlocks'], 1)
        self.assertEqual(block['FileSize'], 150780132)
        self.assertEqual(block['file_size'], 150780132)
        self.assertEqual(block['NumberOfFiles'], 2)
        self.assertEqual(block['NumberOfLumis'], 94)

        with self.assertRaises(DBSReaderError):
            self.dbs.getDBSSummaryInfo(BAD_DATASET)
        with self.assertRaises(DBSReaderError):
            self.dbs.getDBSSummaryInfo(DATASET, BLOCK + 'asas')

    def testListFileBlocks(self):
        """listFileBlocks returns block names in dataset"""
        self.dbs = DBSReader(self.endpoint)
        blocks = self.dbs.listFileBlocks(DATASET)
        self.assertTrue(BLOCK in blocks)
        # block is closed
        block = self.dbs.listFileBlocks(DATASET, blockName=BLOCK)[0]
        self.assertEqual(block, BLOCK)
        self.assertTrue(BLOCK in block)

    def testBlockExists(self):
        """blockExists returns existence of blocks"""
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(self.dbs.blockExists(BLOCK))
        self.assertFalse(self.dbs.blockExists(DATASET + '#somethingelse'))

    def testListFilesInBlock(self):
        """listFilesInBlock returns files in block"""
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(FILE in [x['LogicalFileName'] for x in self.dbs.listFilesInBlock(BLOCK)])
        self.assertRaises(DBSReaderError, self.dbs.listFilesInBlock, DATASET + '#blah')

    def testListFilesInBlockWithParents(self):
        """listFilesInBlockWithParents gets files with parents for a block"""
        self.dbs = DBSReader(self.endpoint)
        files = self.dbs.listFilesInBlockWithParents(
                '/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0')
        self.assertEqual(4, len(files))
        self.assertEqual('/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0',
                         files[0]['block_name'])
        self.assertEqual('/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0',
                         files[0]['BlockName'])
        self.assertEqual(
                '/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/1043E89F-2DCF-E411-9CAE-02163E013751.root',
                files[0]['ParentList'][0]['LogicalFileName'])

        self.assertRaises(DBSReaderError, self.dbs.listFilesInBlockWithParents, BLOCK + 'asas')

    def testLfnsInBlock(self):
        """lfnsInBlock returns lfns in block"""
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(FILE in [x['logical_file_name'] for x in self.dbs.lfnsInBlock(BLOCK)])
        self.assertRaises(DBSReaderError, self.dbs.lfnsInBlock, BLOCK + 'asas')

    def testListFileBlockLocation(self):
        """listFileBlockLocation returns block location"""
        WRONG_BLOCK = BLOCK[:-4] + 'abcd'
        BLOCK2 = '/HighPileUp/Run2011A-v1/RAW#6021175e-cbfb-11e0-80a9-003048caaace'
        DBS_BLOCK = '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-' + \
                    'ea0972193530f531086947d06eb0f121/USER#fb978442-a61b-413a-b4f4-526e6cdb142e'
        DBS_BLOCK2 = '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-' + \
                     'ea0972193530f531086947d06eb0f121/USER#0b04d417-d734-4ef2-88b0-392c48254dab'
        self.dbs = DBSReader('https://cmsweb-prod.cern.ch/dbs/prod/phys03/DBSReader/')

        self.assertEqual(self.dbs.listFileBlockLocation(BLOCK), [])
        # This block is only found on DBS
        self.assertItemsEqual(self.dbs.listFileBlockLocation(DBS_BLOCK), [u'T2_ES_CIEMAT'])
        # doesn't raise on non-existant block
        self.assertEqual(self.dbs.listFileBlockLocation(WRONG_BLOCK), [])
        # test bulk call:
        ## two blocks in phedex
        self.assertEqual(2, len(self.dbs.listFileBlockLocation([BLOCK, BLOCK2])))
        ## one block in phedex one does not exist
        self.assertEqual(2, len(self.dbs.listFileBlockLocation([BLOCK, WRONG_BLOCK])))
        ## one in phedex one in dbs
        self.assertEqual(2, len(self.dbs.listFileBlockLocation([BLOCK, DBS_BLOCK])))
        ## two in dbs
        self.assertEqual(2, len(self.dbs.listFileBlockLocation([DBS_BLOCK, DBS_BLOCK2])))
        ## one in DBS and one does not exist
        self.assertEqual(2, len(self.dbs.listFileBlockLocation([DBS_BLOCK, WRONG_BLOCK])))

    def testGetFileBlock(self):
        """getFileBlock returns block"""
        self.dbs = DBSReader(self.endpoint)
        block = self.dbs.getFileBlock(BLOCK)
        self.assertEqual(len(block), 2)
        self.assertEqual(2, len(block['Files']))

        self.assertRaises(DBSReaderError, self.dbs.getFileBlock, BLOCK + 'asas')

    def testGetFileBlockWithParents(self):
        """getFileBlockWithParents returns block and parents"""
        self.dbs = DBSReader(self.endpoint)
        block = self.dbs.getFileBlockWithParents(BLOCK_WITH_PARENTS)
        self.assertEqual(len(block), 2)
        self.assertEqual(PARENT_FILE, block['Files'][0]['ParentList'][0]['LogicalFileName'])

        self.assertRaises(DBSReaderError, self.dbs.getFileBlockWithParents, BLOCK + 'asas')

    def testListBlockParents(self):
        """listBlockParents returns block parents"""
        self.dbs = DBSReader(self.endpoint)
        parents = self.dbs.listBlockParents(BLOCK_WITH_PARENTS)
        self.assertItemsEqual([PARENT_BLOCK], parents)

        self.assertFalse(self.dbs.listBlockParents(PARENT_BLOCK))

    def testBlockToDatasetPath(self):
        """blockToDatasetPath extracts path from block name"""
        self.dbs = DBSReader(self.endpoint)
        self.assertEqual(self.dbs.blockToDatasetPath(BLOCK), DATASET)
        self.assertRaises(DBSReaderError, self.dbs.blockToDatasetPath, BLOCK + 'asas')



    ### NEW UNIT TESTS ###
    def testCheckDBSServer(self):
        """Test the checkDBSServer method"""
        # Go and Python based servers return different data in different format
        # just check whether something is returned
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(self.dbs.checkDBSServer())

    def testCrossCheck(self):
        """Test the crossCheck method"""
        self.dbs = DBSReader(self.endpoint)
        listFiles = [FILE, FILE1_WITH_PARENT]
        results = self.dbs.crossCheck(DATASET, *listFiles)
        self.assertItemsEqual(results, [FILE])

    def testCrossCheckMissing(self):
        """Test the crossCheckMissing method"""
        self.dbs = DBSReader(self.endpoint)
        listFiles = [FILE, FILE1_WITH_PARENT]
        results = self.dbs.crossCheckMissing(DATASET, *listFiles)
        self.assertItemsEqual(results, [FILE1_WITH_PARENT])

    def testListDatasetLocation(self):
        """Test the listDatasetLocation method"""
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.listDatasetLocation(DATASET)
        self.assertItemsEqual(results, [])

    def testCheckDatasetPath(self):
        """Test the checkDatasetPath method"""
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.checkDatasetPath(DATASET)
        self.assertIsNone(results)

        with self.assertRaises(DBSReaderError):
            self.dbs.checkDatasetPath("")
        with self.assertRaises(DBSReaderError):
            self.dbs.checkDatasetPath(None)
        with self.assertRaises(DBSReaderError):
            self.dbs.checkDatasetPath(BAD_DATASET)

    def testCheckBlockName(self):
        """Test the checkBlockName method"""
        self.dbs = DBSReader(self.endpoint)
        self.assertIsNone(self.dbs.checkBlockName(DATASET))
        self.assertIsNone(self.dbs.checkBlockName(BLOCK))
        self.assertIsNone(self.dbs.checkBlockName("blah"))

        with self.assertRaises(DBSReaderError):
            self.assertIsNone(self.dbs.checkBlockName("*"))
        with self.assertRaises(DBSReaderError):
            self.assertIsNone(self.dbs.checkBlockName(""))
        with self.assertRaises(DBSReaderError):
            self.assertIsNone(self.dbs.checkBlockName(None))

    def testGetFileListByDataset(self):
        """Test the getFileListByDataset method"""
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.getFileListByDataset(DATASET, detail=False)
        self.assertEqual(len(results), 49)
        self.assertEqual(len(results[0]), 1)
        self.assertTrue('logical_file_name' in results[0])
        self.assertTrue(results[0]['logical_file_name'].startswith("/store/data/Run2011A/HighPileUp/RAW/v1"))

        results = self.dbs.getFileListByDataset(DATASET, detail=True)
        self.assertEqual(len(results), 49)
        self.assertEqual(len(results[0]), 20)  # 20 keys in total
        subSetKeys = {'block_name', 'dataset', 'event_count', 'file_size', 'logical_file_name'}
        self.assertTrue(subSetKeys.issubset(results[0]))

    def testListDatasetParents(self):
        """Test the listDatasetParents method"""
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.listDatasetParents(DATASET_WITH_PARENTS)
        self.assertEqual(results[0]['this_dataset'], DATASET_WITH_PARENTS)
        self.assertEqual(results[0]['parent_dataset'], PARENT_DATASET)

        results = self.dbs.listDatasetParents(PARENT_DATASET)
        self.assertEqual(results, [])

        with self.assertRaises(DBSReaderError):
            self.dbs.listDatasetParents(BLOCK)

    @attr('integration')  # too much data to mock
    def testGetParentFilesGivenParentDataset(self):
        """Test the getParentFilesGivenParentDataset method"""
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.getParentFilesGivenParentDataset(PARENT_DATASET,
                                                            [FILE1_WITH_PARENT, FILE2_WITH_PARENT])
        self.assertItemsEqual(results[FILE1_WITH_PARENT], {'/store/data/ComissioningHI/Cosmics/RAW/v1/000/180/841/721E482F-A407-E111-8C0C-BCAEC518FF6E.root'})
        self.assertItemsEqual(results[FILE2_WITH_PARENT], {'/store/data/ComissioningHI/Cosmics/RAW/v1/000/180/852/C0A3D337-1408-E111-836F-0030486780A8.root'})

    def testGetParentFilesByLumi(self):
        """Test the getParentFilesByLumi method"""
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.getParentFilesByLumi(FILE1_WITH_PARENT)
        self.assertItemsEqual(results[0]['ParentDataset'], PARENT_DATASET)
        self.assertItemsEqual(results[0]['ParentFiles'], ['/store/data/ComissioningHI/Cosmics/RAW/v1/000/180/841/721E482F-A407-E111-8C0C-BCAEC518FF6E.root'])

    @attr('integration')  # too much data to mock
    def testListBlocksWithNoParents(self):
        """Test the listBlocksWithNoParents method"""
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.listBlocksWithNoParents(DATASET_WITH_PARENTS)
        self.assertItemsEqual(results, [])

        results = self.dbs.listBlocksWithNoParents(DATASET)
        self.assertEqual(len(results), 46)
        self.assertTrue('/HighPileUp/Run2011A-v1/RAW#6ffd4f16-cc42-11e0-80a9-003048caaace' in results)

    def testListFilesWithNoParents(self):
        """Test the listFilesWithNoParents method"""
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.listFilesWithNoParents(BLOCK_WITH_PARENTS)
        self.assertEqual(len(results), 0)

        results = self.dbs.listFilesWithNoParents(BLOCK)
        self.assertEqual(len(results), 2)
        self.assertTrue('/store/data/Run2011A/HighPileUp/RAW/v1/000/173/657/B293AF24-BFCB-E011-8F85-BCAEC5329701.root' in results)

    def testGetParentDatasetTrio(self):
        """Test the getParentDatasetTrio method"""
        self.dbs = DBSReader(self.endpoint)
        results = self.dbs.getParentDatasetTrio(DATASET_WITH_PARENTS)
        self.assertTrue(frozenset({2, 180851}) in results)
        self.assertEqual(int(results[frozenset({2, 180851})]), 36092526)
        self.assertTrue(frozenset({1, 180851}) in results)
        self.assertEqual(int(results[frozenset({2, 180851})]), 36092526)


if __name__ == '__main__':
    unittest.main()
