#!/usr/bin/env python
"""
_DBSReader_t_

Unit test for the DBS helper class.
"""

import unittest
from nose.plugins.attrib import attr
from functools import wraps

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
        self.dbs      = None
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

    @attr("integration")
    def testlistRuns(self):
        """listRuns returns known runs"""
        self.dbs = DBSReader(self.endpoint)
        runs = self.dbs.listRuns(dataset = DATASET)
        self.assertEqual(46, len(runs))
        self.assertTrue(174074 in runs)
        runs = self.dbs.listRuns(dataset = DATASET, block = BLOCK)
        self.assertEqual([173657], runs)

    @attr("integration")
    def testlistRunLumis(self):
        """listRunLumis returns known runs and lumicounts"""
        self.dbs = DBSReader(self.endpoint)
        runs = self.dbs.listRunLumis(dataset = DATASET)
        self.assertEqual(46, len(runs))
        self.assertTrue(173692 in runs)
        self.assertEqual(runs[173692], 2782)
        runs = self.dbs.listRuns(dataset = DATASET, block = BLOCK)
        self.assertEqual({173657 : 94}, runs)

    @attr("integration")
    def testListProcessedDatasets(self):
        """listProcessedDatasets returns known processed datasets"""
        self.dbs = DBSReader(self.endpoint)
        datasets = self.dbs.listProcessedDatasets('Jet', 'RAW')
        self.assertTrue('Run2011A-v1' in datasets)
        self.assertTrue('Run2011B-v1' in datasets)
        self.assertFalse(self.dbs.listProcessedDatasets('Jet', 'blah'))
        self.assertFalse(self.dbs.listProcessedDatasets('blah', 'RAW'))

    @attr("integration")
    def testlistDatasetFiles(self):
        """listDatasetFiles returns files in dataset"""
        self.dbs = DBSReader(self.endpoint)
        files = self.dbs.listDatasetFiles(DATASET)
        self.assertEqual(49, len(files))
        self.assertTrue(FILE in files)

    @attr("integrtion")
    def testGetDBSSummaryInfo(self):
        """getDBSSummaryInfo returns summary of dataset and block"""
        self.dbs = DBSReader(self.endpoint)
        dataset = self.dbs.getDBSSummaryInfo(DATASET)
        self.assertEqual(dataset['path'], DATASET)
        self.assertEqual(dataset['block'], '')
        self.assertEqual(dataset['NumberOfEvents'], '22075')
        self.assertEqual(dataset['NumberOfBlocks'], '46')
        self.assertEqual(dataset['total_size'], '4001680824')
        self.assertEqual(dataset['NumberOfFiles'], '49')
        self.assertEqual(dataset['NumberOfLumis'], '7223')

        block = self.dbs.getDBSSummaryInfo(DATASET, BLOCK)
        self.assertEqual(block['path'], '')
        self.assertEqual(block['block'], BLOCK)
        self.assertEqual(block['NumberOfEvents'], '377')
        self.assertEqual(block['NumberOfBlocks'], '1')
        self.assertEqual(block['total_size'], '150780132')
        self.assertEqual(block['NumberOfFiles'], '2')
        self.assertEqual(block['NumberOfLumis'], '94')

        self.assertRaises(DBSReaderError, self.dbs.getDBSSummaryInfo, DATASET + 'blah')
        self.assertRaises(DBSReaderError, self.dbs.getDBSSummaryInfo, DATASET, BLOCK + 'asas')

    @attr("integration")
    def testGetFileBlocksInfo(self):
        """getFileBlocksInfo returns block info, including location lookup"""
        self.dbs = DBSReader(self.endpoint)
        blocks = self.dbs.getFileBlocksInfo(DATASET)
        block = self.dbs.getFileBlocksInfo(DATASET, blockName = BLOCK)
        self.assertEqual(1, len(block))
        block = block[0]
        self.assertEqual(46, len(blocks))
        self.assertTrue(block['Name'] in [x['Name'] for x in blocks])
        self.assertEqual(BLOCK, block['Name'])
        #self.assertEqual(377, block['NumberOfEvents'])
        self.assertEqual(150780132, block['BlockSize'])
        self.assertEqual(2, block['NumberOfFiles'])
        # possibly fragile but assume block located at least at cern
        sites = [x['Name'] for x in block['StorageElementList'] if x['Name'].find('cern.ch') > -1]
        self.assertTrue(sites)

        # weird error handling - depends on whether block or dataset is missing
        self.assertRaises(DBSReaderError, self.dbs.getFileBlocksInfo, DATASET + 'blah')
        self.assertFalse(self.dbs.getFileBlocksInfo(DATASET, blockName = BLOCK + 'asas'))

    @attr("integration")
    def testListFileBlocks(self):
        """listFileBlocks returns block names in dataset"""
        self.dbs = DBSReader(self.endpoint)
        blocks = self.dbs.listFileBlocks(DATASET)
        # block is closed
        block = self.dbs.listFileBlocks(DATASET, blockName = BLOCK, onlyClosedBlocks = True)[0]
        self.assertEqual(block, BLOCK)
        self.assertTrue(BLOCK in block)

    @attr("integration")
    def testListOpenFileBlocks(self):
        """listOpenFileBlocks finds open blocks"""
        # hard to find a dataset with open blocks, so don't bother
        self.dbs = DBSReader(self.endpoint)
        self.assertFalse(self.dbs.listOpenFileBlocks(DATASET))

    @attr("integration")
    def testBlockExists(self):
        """blockExists returns existence of blocks"""
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(self.dbs.blockExists(BLOCK))
        self.assertFalse(self.dbs.blockExists(DATASET + '#somethingelse'))

    @attr("integration")
    def testListFilesInBlock(self):
        """listFilesInBlock returns files in block"""
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(FILE in [x['LogicalFileName'] for x in self.dbs.listFilesInBlock(BLOCK)])
        self.assertRaises(DBSReaderError, self.dbs.listFilesInBlock, DATASET + '#blah')

    @attr("integration")
    def testListFilesInBlockWithParents(self):
        """listFilesInBlockWithParents gets files with parents for a block"""
        # hope PromptReco doesn't get deleted
        self.dbs = DBSReader(self.endpoint)
        files = self.dbs.listFilesInBlockWithParents('/Jet/Run2011A-PromptReco-v1/RECO#f8d36af3-4fb6-11e0-9d39-00151755cb60')
        self.assertEqual(1, len(files))
        self.assertEqual('/Jet/Run2011A-PromptReco-v1/RECO#f8d36af3-4fb6-11e0-9d39-00151755cb60', files[0]['Block']['Name'])
        self.assertEqual('/store/data/Run2011A/Jet/RAW/v1/000/160/433/24B46223-0D4E-E011-B573-0030487C778E.root',
                         files[0]['ParentList'][0]['LogicalFileName'])

        self.assertRaises(DBSReaderError, self.dbs.listFilesInBlockWithParents, BLOCK + 'asas')

    @attr("integration")
    def testLfnsInBlock(self):
        """lfnsInBlock returns lfns in block"""
        self.dbs = DBSReader(self.endpoint)
        self.assertTrue(FILE in self.dbs.lfnsInBlock(BLOCK))
        self.assertRaises(DBSReaderError, self.dbs.lfnsInBlock, BLOCK + 'asas')

    @attr("integration")
    def testListFileBlockLocation(self):
        """listFileBlockLocation returns block location"""
        self.dbs = DBSReader(self.endpoint)
        # assume one site is cern
        sites = [x for x in self.dbs.listFileBlockLocation(BLOCK) if x.find('cern.ch') > -1]
        self.assertTrue(sites)
        # doesn't raise on non-existant block
        self.assertFalse(self.dbs.listFileBlockLocation(BLOCK + 'blah'))

    @attr("integration")
    def testGetFileBlock(self):
        """getFileBlock returns block"""
        self.dbs = DBSReader(self.endpoint)
        block = self.dbs.getFileBlock(BLOCK)
        self.assertEqual(len(block), 1)
        block = block[BLOCK]
        self.assertEqual(2, len(block['Files']))

        self.assertRaises(DBSReaderError, self.dbs.getFileBlock, BLOCK + 'asas')

    @attr("integration")
    def testGetFileBlockWithParents(self):
        """getFileBlockWithParents returns block and parents"""
        self.dbs = DBSReader(self.endpoint)
        block = self.dbs.getFileBlockWithParents('/Jet/Run2011A-PromptReco-v1/RECO#f8d36af3-4fb6-11e0-9d39-00151755cb60')
        self.assertEqual(len(block), 1)
        block = block['/Jet/Run2011A-PromptReco-v1/RECO#f8d36af3-4fb6-11e0-9d39-00151755cb60']
        self.assertEqual('/store/data/Run2011A/Jet/RAW/v1/000/160/433/24B46223-0D4E-E011-B573-0030487C778E.root',
                         block['Files'][0]['ParentList'][0]['LogicalFileName'])

        self.assertRaises(DBSReaderError, self.dbs.getFileBlockWithParents, BLOCK + 'asas')

    @attr("integration")
    def testGetFiles(self):
        """getFiles returns files in dataset"""
        self.dbs = DBSReader(self.endpoint)
        files = self.dbs.getFiles(DATASET)
        self.assertEqual(len(files), 46)

    @attr("integration")
    def testListBlockParents(self):
        """listBlockParents returns block parents"""
        self.dbs = DBSReader(self.endpoint)
        parents = self.dbs.listBlockParents('/Jet/Run2011A-PromptReco-v1/RECO#f8d36af3-4fb6-11e0-9d39-00151755cb60')
        self.assertEqual(1, len(parents))
        self.assertEqual('/Jet/Run2011A-v1/RAW#37cf2a40-4e0e-11e0-9833-00151755cb60',
                         parents[0]['Name'])
        sites = [x for x in parents[0]['StorageElementList'] if x.find("cern.ch") > -1]
        self.assertTrue(sites)

        self.assertFalse(self.dbs.listBlockParents('/Jet/Run2011A-PromptReco-v1/RECO#f8d36af3-4fb6-11e0-9d39-00151755cb60dsl'))

    @attr("integration")
    def testBlockIsOpen(self):
        """blockIsOpen checks if a block is open"""
        self.dbs = DBSReader(self.endpoint)
        self.assertFalse(self.dbs.blockIsOpen(BLOCK))

    @attr("integration")
    def testBlockToDatasetPath(self):
        """blockToDatasetPath extracts path from block name"""
        self.dbs = DBSReader(self.endpoint)
        self.assertEqual(self.dbs.blockToDatasetPath(BLOCK), DATASET)
        self.assertFalse(self.dbs.blockToDatasetPath(BLOCK + 'asas'))



if __name__ == '__main__':
    unittest.main()
