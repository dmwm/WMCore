"""
_Tier0Plugin_t_

Test the Tier-0 plugin for the AnalyticsDataCollector

Created on Nov 7, 2012

@author: dballest
"""
from __future__ import division
from builtins import range

import os
import unittest

from WMComponent.AnalyticsDataCollector.Plugins.Tier0Plugin import Tier0Plugin
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.StdSpecs.PromptReco import PromptRecoWorkloadFactory
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit


class Tier0PluginTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Setup the test environment
        """
        self.testInit = TestInit(__file__)
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(["WMCore.WMBS"])
        self.requestCouchDB = 'wmstats_plugin_t'
        self.testInit.setupCouch(self.requestCouchDB, 'T0Request')
        self.testDir = self.testInit.generateWorkDir()
        reqDBURL = "%s/%s" % (os.environ['COUCHURL'], self.requestCouchDB)
        self.requestDBWriter = RequestDBWriter(reqDBURL, couchapp="T0Request")
        self.requestDBWriter._setNoStale()

        self.stateMap = {}
        self.orderedStates = []
        self.plugin = None

        return

    def tearDown(self):
        """
        _tearDown_

        Clear databases and delete files
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()

        return

    def setupRepackWorkflow(self):
        """
        _setupRepackWorkflow_

        Populate WMBS with a repack-like workflow,
        every subscription must be unfinished at first
        """

        workflowName = 'Repack_Run481516_StreamZ'
        mergeTasks = ['RepackMergewrite_QuadElectron_RAW', 'RepackMergewrite_TriPhoton_RAW',
                      'RepackMergewrite_SingleNeutrino_RAW']

        self.stateMap = {'Merge': [],
                         'Processing Done': []}
        self.orderedStates = ['Merge', 'Processing Done']

        # Populate WMStats
        self.requestDBWriter.insertGenericRequest({'RequestName': workflowName})
        self.requestDBWriter.updateRequestStatus(workflowName, 'Closed')

        # Create a wmspec in disk
        workload = newWorkload(workflowName)
        repackTask = workload.newTask('Repack')
        for task in mergeTasks:
            repackTask.addTask(task)
        repackTask.addTask('RepackCleanupUnmergedwrite_QuadElectron_RAW')

        specPath = os.path.join(self.testDir, 'Repack.pkl')
        workload.save(specPath)

        # Populate WMBS
        topFileset = Fileset(name='TestStreamerFileset')
        topFileset.create()

        options = {'spec': specPath, 'owner': 'ItsAMeMario',
                   'name': workflowName, 'wfType': 'tier0'}
        topLevelWorkflow = Workflow(task='/%s/Repack' % workflowName,
                                    **options)
        topLevelWorkflow.create()
        topLevelSub = Subscription(topFileset, topLevelWorkflow)
        topLevelSub.create()
        self.stateMap['Merge'].append(topFileset)
        for task in mergeTasks:
            mergeWorkflow = Workflow(task='/%s/Repack/%s' % (workflowName, task), **options)
            mergeWorkflow.create()
            unmergedFileset = Fileset(name='TestUnmergedFileset%s' % task)
            unmergedFileset.create()
            mergeSub = Subscription(unmergedFileset, mergeWorkflow)
            mergeSub.create()
            self.stateMap['Processing Done'].append(unmergedFileset)
        cleanupWorkflow = Workflow(task='/Repack_Run481516_StreamZ/Repack/RepackCleanupUnmergedwrite_QuadElectron_RAW',
                                   **options)
        cleanupWorkflow.create()
        unmergedFileset = Fileset(name='TestUnmergedFilesetToCleanup')
        unmergedFileset.create()
        cleanupSub = Subscription(unmergedFileset, cleanupWorkflow)
        cleanupSub.create()

        return

    def setupExpressWorkflow(self):
        """
        _setupExpressWorkflow_

        Populate WMBS with a express-like workflow,
        every subscription must be unfinished at first
        """

        workflowName = 'Express_Run481516_StreamZFast'
        secondLevelTasks = ['ExpressMergewrite_StreamZFast_DQM', 'ExpressMergewrite_ExpressPhysics_FEVT',
                            'ExpressAlcaSkimwrite_StreamZFast_ALCARECO', 'ExpressCleanupUnmergedwrite_StreamZFast_DQM',
                            'ExpressCleanupUnmergedwrite_ExpressPhysics_FEVT',
                            'ExpressCleanupUnmergedwrite_StreamZFast_ALCARECO']
        alcaHarvestTask = 'ExpressAlcaSkimwrite_StreamZFast_ALCARECOAlcaHarvestALCARECOStreamPromptCalibProd'
        dqmHarvestTask = 'ExpressMergewrite_StreamZFast_DQMEndOfRunDQMHarvestMerged'

        self.stateMap = {'Merge': [],
                         'Harvesting': [],
                         'Processing Done': []}
        self.orderedStates = ['Merge', 'Harvesting', 'Processing Done']

        # Populate WMStats
        self.requestDBWriter.insertGenericRequest({'RequestName': workflowName})
        self.requestDBWriter.updateRequestStatus(workflowName, 'Closed')

        # Create a wmspec in disk
        workload = newWorkload(workflowName)
        expressTask = workload.newTask('Express')
        for task in secondLevelTasks:
            secondLevelTask = expressTask.addTask(task)
            if task == 'ExpressAlcaSkimwrite_StreamZFast_ALCARECO':
                secondLevelTask.addTask(alcaHarvestTask)
            elif task == 'ExpressMergewrite_StreamZFast_DQM':
                secondLevelTask.addTask(dqmHarvestTask)

        specPath = os.path.join(self.testDir, 'Express.pkl')
        workload.save(specPath)

        # Populate WMBS
        sharedFileset = Fileset(name='TestFileset')
        sharedFileset.create()
        sharedFileset.markOpen(False)

        options = {'spec': specPath, 'owner': 'ItsAMeMario',
                   'name': workflowName, 'wfType': 'tier0'}
        topLevelWorkflow = Workflow(task='/%s/Express' % workflowName,
                                    **options)
        topLevelWorkflow.create()
        topLevelSub = Subscription(sharedFileset, topLevelWorkflow)
        topLevelSub.create()
        self.stateMap['Merge'].append(topLevelSub)
        for task in [x for x in secondLevelTasks if not x.count('CleanupUnmerged')]:
            secondLevelWorkflow = Workflow(task='/%s/Express/%s' % (workflowName, task), **options)
            secondLevelWorkflow.create()
            mergeSub = Subscription(sharedFileset, secondLevelWorkflow)
            mergeSub.create()
            self.stateMap['Harvesting'].append(mergeSub)

        for (parent, child) in [('ExpressAlcaSkimwrite_StreamZFast_ALCARECO', alcaHarvestTask),
                                ('ExpressMergewrite_StreamZFast_DQM', dqmHarvestTask)]:
            harvestingWorkflow = Workflow(task='/%s/Express/%s/%s' % (workflowName, parent, child),
                                          **options)
            harvestingWorkflow.create()
            harvestingSub = Subscription(sharedFileset, harvestingWorkflow)
            harvestingSub.create()
            self.stateMap['Processing Done'].append(harvestingSub)

        return

    def setupPromptRecoWorkflow(self):
        """
        _setupPromptRecoWorkflow_

        Populate WMBS with a real PromptReco workflow,
        every subscription must be unfinished at first
        """

        # Populate disk and WMBS
        testArguments = PromptRecoWorkloadFactory.getTestArguments()

        workflowName = 'PromptReco_Run195360_Cosmics'
        factory = PromptRecoWorkloadFactory()
        testArguments["EnableHarvesting"] = True
        testArguments["CouchURL"] = os.environ["COUCHURL"]
        workload = factory.factoryWorkloadConstruction(workflowName, testArguments)

        wmbsHelper = WMBSHelper(workload, 'Reco', 'SomeBlock', cachepath=self.testDir)
        wmbsHelper.createTopLevelFileset()
        wmbsHelper._createSubscriptionsInWMBS(wmbsHelper.topLevelTask, wmbsHelper.topLevelFileset)

        self.stateMap = {'AlcaSkim': [],
                         'Merge': [],
                         'Harvesting': [],
                         'Processing Done': []}
        self.orderedStates = ['AlcaSkim', 'Merge', 'Harvesting', 'Processing Done']

        # Populate WMStats
        self.requestDBWriter.insertGenericRequest({'RequestName': workflowName})
        self.requestDBWriter.updateRequestStatus(workflowName, 'Closed')

        topLevelTask = '/%s/Reco' % workflowName
        alcaSkimTask = '%s/AlcaSkim' % topLevelTask
        mergeTasks = ['%s/AlcaSkim/AlcaSkimMergeALCARECOStreamHcalCalHOCosmics',
                      '%s/AlcaSkim/AlcaSkimMergeALCARECOStreamTkAlCosmics0T',
                      '%s/AlcaSkim/AlcaSkimMergeALCARECOStreamMuAlGlobalCosmics',
                      '%s/RecoMergewrite_AOD',
                      '%s/RecoMergewrite_DQM',
                      '%s/RecoMergewrite_RECO']
        harvestingTask = '%s/RecoMergewrite_DQM/RecoMergewrite_DQMEndOfRunDQMHarvestMerged' % topLevelTask

        self.stateMap['AlcaSkim'].append(wmbsHelper.topLevelSubscription)

        alcaSkimWorkflow = Workflow(name=workflowName, task=alcaSkimTask)
        alcaSkimWorkflow.load()
        alcarecoFileset = Fileset(name='/PromptReco_Run195360_Cosmics/Reco/unmerged-write_ALCARECOALCARECO')
        alcarecoFileset.load()
        alcaSkimSub = Subscription(alcarecoFileset, alcaSkimWorkflow)
        alcaSkimSub.load()
        self.stateMap['Merge'].append(alcaSkimSub)

        for task in mergeTasks:
            mergeTask = task % topLevelTask
            mergeWorkflow = Workflow(name=workflowName, task=mergeTask)
            mergeWorkflow.load()
            if 'AlcaSkim' in mergeTask:
                stream = mergeTask.split('/')[-1][13:]
                unmergedFileset = Fileset(name='%s/unmerged-%sALCARECO' % (alcaSkimTask, stream))
                unmergedFileset.load()
            else:
                dataTier = mergeTask.split('/')[-1].split('_')[-1]
                unmergedFileset = Fileset(name='%s/unmerged-write_%s%s' % (topLevelTask, dataTier, dataTier))
                unmergedFileset.load()
            mergeSub = Subscription(unmergedFileset, mergeWorkflow)
            mergeSub.load()
            self.stateMap['Harvesting'].append(mergeSub)

        harvestingWorkflow = Workflow(name=workflowName, task=harvestingTask)
        harvestingWorkflow.load()
        harvestingFileset = Fileset(name='/PromptReco_Run195360_Cosmics/Reco/RecoMergewrite_DQM/merged-MergedDQM')
        harvestingFileset.load()
        harvestingSub = Subscription(harvestingFileset, harvestingWorkflow)
        harvestingSub.load()
        self.stateMap['Processing Done'].append(harvestingSub)

        return

    def verifyStateTransitions(self, transitionMethod='markFinished', transitionTrigger=True):
        """
        _verifyStateTransitions_

        Utility method which goes through the list of states in self.orderedStates and
        finishes the tasks that demand a state transition in each step. This according
        to the defined transition method and trigger.
        It verifies that the request document in WMStats is moving according to the transitions
        """

        for idx in range(0, len(self.orderedStates) * 2):
            nextState = self.orderedStates[idx // 2]
            if (idx // 2) == 0:
                currentState = 'Closed'
            else:
                currentState = self.orderedStates[idx // 2 - 1]
            if idx % 2 == 0:
                for transitionObject in self.stateMap[nextState][:-1]:
                    method = getattr(transitionObject, transitionMethod)
                    method(transitionTrigger)
                self.plugin([], self.requestDBWriter, self.requestDBWriter)
                currentStateWorkflows = self.requestDBWriter.getRequestByStatus([currentState])
                nextStateWorkflows = self.requestDBWriter.getRequestByStatus([nextState])
                self.assertEqual(len(currentStateWorkflows), 1, 'Workflow moved incorrectly from %s' % currentState)
                self.assertEqual(len(nextStateWorkflows), 0, 'Workflow moved incorrectly to %s' % nextState)
            else:
                transitionObject = self.stateMap[nextState][-1]
                method = getattr(transitionObject, transitionMethod)
                method(transitionTrigger)
                self.plugin([], self.requestDBWriter, self.requestDBWriter)
                currentStateWorkflows = self.requestDBWriter.getRequestByStatus([currentState])
                nextStateWorkflows = self.requestDBWriter.getRequestByStatus([nextState])
                self.assertEqual(len(currentStateWorkflows), 0,
                                 'Workflow did not move correctly from %s' % currentState)
                self.assertEqual(len(nextStateWorkflows), 1, 'Workflow did not move correctly to %s' % nextState)
        return

    def testA_RepackStates(self):
        """
        _testA_RepackStates_

        Setup an environment with a Repack workflow
        and traverse through the different states.
        Check that the transitions are sane.
        """
        # Set the environment
        self.setupRepackWorkflow()
        self.plugin = Tier0Plugin()

        # Verify the transitions
        self.verifyStateTransitions('markOpen', False)

        return

    def testB_ExpressStates(self):
        """
        _testB_ExpressStates_

        Setup an environment with a Express workflow
        and traverse through the different states.
        Check that the transitions are sane.
        """
        # Set the environment
        self.setupExpressWorkflow()
        self.plugin = Tier0Plugin()

        # Verify the transitions
        self.verifyStateTransitions()

        return

    def testC_PromptRecoStates(self):
        """
        _testC_PromptRecoStates_

        Setup an environment with a PromptReco workflow
        and traverse through the different states.
        Check that the transitions are sane.
        """
        # Set the environment
        self.setupPromptRecoWorkflow()
        self.plugin = Tier0Plugin()

        # Verify the transitions
        self.verifyStateTransitions()

        return


if __name__ == "__main__":
    unittest.main()
