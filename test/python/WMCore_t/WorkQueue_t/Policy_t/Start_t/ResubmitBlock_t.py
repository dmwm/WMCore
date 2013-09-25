#!/usr/bin/env python
"""
_ResubmitBlock_t_

WorkQueue ResubmitBlock tests

Created on Feb 19, 2013

@author: dballest
"""

import os
import unittest

from random import choice

from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run
from WMCore.GroupUser.User import makeUser
from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WorkQueue.Policy.Start.ResubmitBlock import ResubmitBlock
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueNoWorkError, WorkQueueWMSpecError

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import createConfig

class ResubmitBlockTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Setup couchdb and the test environment
        """
        # Set external test helpers
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setupCouch("resubmitblock_t", "ACDC", "GroupUser")
        EmulatorHelper.setEmulators(siteDB = True)

        # Define test environment
        self.couchUrl = os.environ["COUCHURL"]
        self.acdcDBName = 'resubmitblock_t'
        self.validLocations = ['srm-cms.gridpp.rl.ac.uk', 'cmssrm.fnal.gov', 'srm.unl.edu']
        self.validLocationsCMSNames = ['T2_US_Nebraska', 'T1_US_FNAL', 'T1_UK_RAL']
        self.siteWhitelist = ['T2_XX_SiteA']
        self.workflowName = 'dballest_ReReco_workflow'
        couchServer = CouchServer(dburl = self.couchUrl)
        self.acdcDB = couchServer.connectDatabase(self.acdcDBName, create = False)
        user = makeUser('unknown', 'sfoulkes@fnal.gov', self.couchUrl, self.acdcDBName)
        user.create()

        return

    def tearDown(self):
        """
        _tearDown_

        Clean couchdb and test environment
        """
        self.testInit.tearDownCouch()
        EmulatorHelper.resetEmulators()
        return

    def getProcessingACDCSpec(self, splittingAlgo = 'LumiBased', splittingArgs = {'lumis_per_job' : 8},
                              setLocationFlag = False):
        """
        _getProcessingACDCSpec_

        Get a ACDC spec for the processing task of a ReReco workload
        """
        factory = ReRecoWorkloadFactory()
        rerecoArgs = ReRecoWorkloadFactory.getTestArguments()
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        Tier1ReRecoWorkload = factory.factoryWorkloadConstruction(self.workflowName, rerecoArgs)
        Tier1ReRecoWorkload.truncate('ACDC_%s' % self.workflowName, '/%s/DataProcessing' % self.workflowName, self.couchUrl,
                                     self.acdcDBName)
        Tier1ReRecoWorkload.setJobSplittingParameters('/ACDC_%s/DataProcessing' % self.workflowName, splittingAlgo, splittingArgs)
        if setLocationFlag:
            Tier1ReRecoWorkload.setLocationDataSourceFlag()
            Tier1ReRecoWorkload.setSiteWhitelist(self.siteWhitelist)
        return Tier1ReRecoWorkload

    def getMergeACDCSpec(self, splittingAlgo = 'ParentlessMergeBySize', splittingArgs = {}):
        """
        _getMergeACDCSpec_

        Get a ACDC spec for the merge task of a ReReco workload
        """
        factory = ReRecoWorkloadFactory()
        rerecoArgs = ReRecoWorkloadFactory.getTestArguments()
        rerecoArgs["ConfigCacheID"] = createConfig(rerecoArgs["CouchDBName"])
        Tier1ReRecoWorkload = factory.factoryWorkloadConstruction(self.workflowName, rerecoArgs)
        Tier1ReRecoWorkload.truncate('ACDC_%s' % self.workflowName, '/%s/DataProcessing/DataProcessingMergeRECOoutput' % self.workflowName,
                                     self.couchUrl, self.acdcDBName)
        Tier1ReRecoWorkload.setJobSplittingParameters('/ACDC_%s/DataProcessingMergeRECOoutput' % self.workflowName,
                                                      splittingAlgo, splittingArgs)
        return Tier1ReRecoWorkload

    def stuffACDCDatabase(self, numFiles = 50, lumisPerFile = 20, lumisPerACDCRecord = 2):
        """
        _stuffACDCDatabase_

        Fill the ACDC database with ACDC records, both for processing
        and merge
        """
        filesetName = '/%s/DataProcessing' % self.workflowName
        owner = 'unknown'
        group = 'unknown'


        for i in range(numFiles):
            for j in range(1, lumisPerFile + 1, lumisPerACDCRecord):
                lfn = '/store/data/a/%d' % i
                acdcFile = File(lfn = lfn, size = 100, events = 250, locations = self.validLocations, merged = 1)
                run = Run(i + 1, *range(j, min(j + lumisPerACDCRecord, lumisPerFile + 1)))
                acdcFile.addRun(run)
                acdcDoc = {'collection_name' : self.workflowName,
                           'collection_type' : 'ACDC.CollectionTypes.DataCollection',
                           'files' : {lfn : acdcFile},
                           'fileset_name' : filesetName,
                           'owner' : {'user': owner,
                                      'group' : group}}
                self.acdcDB.queue(acdcDoc)
        filesetName = '/%s/DataProcessing/DataProcessingMergeRECOoutput' % self.workflowName

        for i in range(numFiles):
            for j in range(1, lumisPerFile + 1, lumisPerACDCRecord):
                lfn = '/store/unmerged/b/%d' % i
                acdcFile = File(lfn = lfn, size = 100, events = 250, locations = set([choice(self.validLocations)]), merged = 0)
                run = Run(i + 1, *range(j, min(j + lumisPerACDCRecord, lumisPerFile + 1)))
                acdcFile.addRun(run)
                acdcDoc = {'collection_name' : self.workflowName,
                           'collection_type' : 'ACDC.CollectionTypes.DataCollection',
                           'files' : {lfn : acdcFile},
                           'fileset_name' : filesetName,
                           'owner' : {'user': owner,
                                      'group' : group}}
                self.acdcDB.queue(acdcDoc)
        self.acdcDB.commit()

        return

    def testEmptyACDC(self):
        """
        _testEmptyACDC_

        Test that the correct exception is rased when trying an ACDC
        without any ACDC documents. Must test a splittingAlgo that
        works in fixedSizeChunks and singleChunk.
        """
        # SingleChunk
        acdcWorkload = self.getProcessingACDCSpec()
        for task in acdcWorkload.taskIterator():
            policy = ResubmitBlock()
            self.assertRaises(WorkQueueNoWorkError, policy, acdcWorkload, task)

        # FixedSizeChunks
        acdcWorkload = self.getProcessingACDCSpec('FileBased', {'files_per_job' : 2000})
        for task in acdcWorkload.taskIterator():
            policy = ResubmitBlock()
            self.assertRaises(WorkQueueNoWorkError, policy, acdcWorkload, task)

        return

    def testUnsupportedACDC(self):
        """
        _testUnsupportedACDC_

        Some tasks with certain algorithms won't work if resubmitted,
        catch those
        """
        acdcWorkload = self.getMergeACDCSpec('WMBSMergeBySize', {})
        for task in acdcWorkload.taskIterator():
            policy = ResubmitBlock()
            self.assertRaises(WorkQueueWMSpecError, policy, acdcWorkload, task)

        return

    def testFixedSizeChunksSplit(self):
        """
        _testFixedSizeChunksSplit_

        Test splitting a resubmit block in fixed size chunks of 250, it will
        use a ReReco ACDC of the processing task.
        """
        self.stuffACDCDatabase()
        acdcWorkload = self.getProcessingACDCSpec('FileBased', {'files_per_job' : 5})
        acdcWorkload.data.request.priority = 10000
        for task in acdcWorkload.taskIterator():
            policy = ResubmitBlock()
            units, _ = policy(acdcWorkload, task)
            self.assertEqual(len(units), 2)
            for unit in units:
                self.assertEqual(len(unit['Inputs']), 1)
                inputBlock = unit['Inputs'].keys()[0]
                self.assertEqual(sorted(unit['Inputs'][inputBlock]), sorted(self.validLocationsCMSNames))
                self.assertEqual(10000, unit['Priority'])
                self.assertEqual(50, unit['Jobs'])
                self.assertEqual(acdcWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(500, unit['NumberOfLumis'])
                self.assertEqual(250, unit['NumberOfFiles'])
                self.assertEqual(62500, unit['NumberOfEvents'])
                self.assertEqual(unit['ACDC'], {'database' : self.acdcDBName,
                                                'fileset' : '/%s/DataProcessing' % self.workflowName,
                                                'collection' : self.workflowName,
                                                'server' : self.couchUrl})
        return

    def testSingleChunksSplit(self):
        """
        _testSingleChunksSplit_

        Test splitting a resubmit block in a single chunk, it will
        use a ReReco ACDC of the merge task and the processing task.
        This is to test two algorithms and that the split is location
        independent.
        """
        self.stuffACDCDatabase()
        acdcWorkload = self.getProcessingACDCSpec('LumiBased', {'lumis_per_job' : 10})
        acdcWorkload.data.request.priority = 10000
        for task in acdcWorkload.taskIterator():
            policy = ResubmitBlock()
            units, _ = policy(acdcWorkload, task)
            self.assertEqual(len(units), 1)
            for unit in units:
                self.assertEqual(len(unit['Inputs']), 1)
                inputBlock = unit['Inputs'].keys()[0]
                self.assertEqual(sorted(unit['Inputs'][inputBlock]), sorted(self.validLocationsCMSNames))
                self.assertEqual(10000, unit['Priority'])
                self.assertEqual(100, unit['Jobs'])
                self.assertEqual(acdcWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(1000, unit['NumberOfLumis'])
                self.assertEqual(500, unit['NumberOfFiles'])
                self.assertEqual(125000, unit['NumberOfEvents'])
                self.assertEqual(unit['ACDC'], {'database' : self.acdcDBName,
                                                'fileset' : '/%s/DataProcessing' % self.workflowName,
                                                'collection' : self.workflowName,
                                                'server' : self.couchUrl})

        acdcWorkload = self.getMergeACDCSpec('ParentlessMergeBySize', {})
        acdcWorkload.data.request.priority = 10000
        for task in acdcWorkload.taskIterator():
            policy = ResubmitBlock()
            units, _ = policy(acdcWorkload, task)
            self.assertEqual(len(units), 1)
            for unit in units:
                self.assertEqual(len(unit['Inputs']), 1)
                inputBlock = unit['Inputs'].keys()[0]
                self.assertEqual(sorted(unit['Inputs'][inputBlock]), sorted(self.validLocationsCMSNames))
                self.assertEqual(10000, unit['Priority'])
                self.assertEqual(500, unit['Jobs'])
                self.assertEqual(acdcWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(1000, unit['NumberOfLumis'])
                self.assertEqual(500, unit['NumberOfFiles'])
                self.assertEqual(125000, unit['NumberOfEvents'])
                self.assertEqual(unit['ACDC'], {'database' : self.acdcDBName,
                                                'fileset' : '/%s/DataProcessing/DataProcessingMergeRECOoutput' % self.workflowName,
                                                'collection' : self.workflowName,
                                                'server' : self.couchUrl})

        return

    def testLocalQueueACDCSplit(self):
        """
        _testLocalQueueACDCSplit_

        Test that the local queue split (i.e. with something in self.data) is compatible
        with the information, namely the block name, produced by the global split.
        """
        self.stuffACDCDatabase()
        acdcWorkload = self.getProcessingACDCSpec('LumiBased', {'lumis_per_job' : 10})
        acdcWorkload.data.request.priority = 10000
        for task in acdcWorkload.taskIterator():
            policy = ResubmitBlock()
            units, _ = policy(acdcWorkload, task)
            self.assertEqual(len(units), 1)
            for unit in units:
                self.assertEqual(len(unit['Inputs']), 1)
                inputBlock = unit['Inputs'].keys()[0]
                self.assertEqual(sorted(unit['Inputs'][inputBlock]), sorted(self.validLocationsCMSNames))
                self.assertEqual(10000, unit['Priority'])
                self.assertEqual(100, unit['Jobs'])
                self.assertEqual(acdcWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(1000, unit['NumberOfLumis'])
                self.assertEqual(500, unit['NumberOfFiles'])
                self.assertEqual(125000, unit['NumberOfEvents'])
                self.assertEqual(unit['ACDC'], {'database' : self.acdcDBName,
                                                'fileset' : '/%s/DataProcessing' % self.workflowName,
                                                'collection' : self.workflowName,
                                                'server' : self.couchUrl})
                globalUnit = unit
            policy = ResubmitBlock()
            units, _ = policy(acdcWorkload, task, data = globalUnit['Inputs'])
            self.assertEqual(len(units), 1)
            for unit in units:
                self.assertEqual(len(unit['Inputs']), 1)
                inputBlock = unit['Inputs'].keys()[0]
                self.assertEqual(sorted(unit['Inputs'][inputBlock]), sorted(self.validLocationsCMSNames))
                self.assertEqual(10000, unit['Priority'])
                self.assertEqual(100, unit['Jobs'])
                self.assertEqual(acdcWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(1000, unit['NumberOfLumis'])
                self.assertEqual(500, unit['NumberOfFiles'])
                self.assertEqual(125000, unit['NumberOfEvents'])
                self.assertEqual(unit['ACDC'], {'database' : self.acdcDBName,
                                                'fileset' : '/%s/DataProcessing' % self.workflowName,
                                                'collection' : self.workflowName,
                                                'server' : self.couchUrl})
        return

    def testSiteWhitelistsLocation(self):
        """
        _testSiteWhitelistsLocation_

        Test splitting a resubmit block changing the block locations
        according to the given site whitelist.
        """
        self.stuffACDCDatabase()
        acdcWorkload = self.getProcessingACDCSpec('FileBased', {'files_per_job' : 5},
                                                  setLocationFlag = True)
        acdcWorkload.data.request.priority = 10000
        for task in acdcWorkload.taskIterator():
            policy = ResubmitBlock()
            units, _ = policy(acdcWorkload, task)
            self.assertEqual(len(units), 2)
            for unit in units:
                self.assertEqual(len(unit['Inputs']), 1)
                inputBlock = unit['Inputs'].keys()[0]
                self.assertEqual(sorted(unit['Inputs'][inputBlock]), sorted(self.siteWhitelist))
                self.assertEqual(10000, unit['Priority'])
                self.assertEqual(50, unit['Jobs'])
                self.assertEqual(acdcWorkload, unit['WMSpec'])
                self.assertEqual(task, unit['Task'])
                self.assertEqual(500, unit['NumberOfLumis'])
                self.assertEqual(250, unit['NumberOfFiles'])
                self.assertEqual(62500, unit['NumberOfEvents'])
                self.assertEqual(unit['ACDC'], {'database' : self.acdcDBName,
                                                'fileset' : '/%s/DataProcessing' % self.workflowName,
                                                'collection' : self.workflowName,
                                                'server' : self.couchUrl})
        return

if __name__ == "__main__":
    unittest.main()
