"""

Unit tests for the RelValMC workflow

"""


import unittest
import os

from WMCore.WMSpec.StdSpecs.RelValMC import getTestArguments, relValMCWorkload
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document



class RelValMCTest(unittest.TestCase):
    def setUp(self):
        """
        Initialize the database and couch.
        
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("montecarlo_t", "ConfigCache")        
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("rereco_t")        


    def tearDown(self):
        """
        Clear out the database.
        
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()


    def _getConfigBase(self):
        """
        The RelValMC workload is supposed to have the same set of config
        values like MonteCarlo, ReReco and PromptSkim workloads combined
        plus three additional config values:
            - GenConfig - ConfigCacheID of the config for the generation task (MonteCarlo)
            - RecoConfig - ConfigCacheID of the config for the reco step (ReReco)
            - AlcaRecoConfig - ConfigCacheID of the config for the skim/alcareco step (PromptSkim) 
        this base config values are taken from MonteCarlo_t, ReReco_t, no test for PromptSkim_t
        
        configurations will be similar, they'll only differ in the output modules they define.        
        
        """        
        config = Document()
        config["info"] = None
        config["config"] = None
        config["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        config["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        config["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        config["pset_tweak_details"] = None
        return config

    
    def injectGenerationConfig(self):
        """        
        Gen step - Will have one output module. Filter name can be anything,
        the data tier must be "GEN-SIM-RAW"
        
        """        
        config = self._getConfigBase()
        config["pset_tweak_details"] = \
            {"process": {"outputModules_": ["OutputA"],
                         "OutputA": {"dataset": {"filterName": "OutputAFilter",
                                                 "dataTier": "GEN-SIM-RAW"}}}}
        result = self.configDatabase.commitOne(config)
        return result[0]["id"]
    
    
    def injectReconstructionConfig(self):
        """
        Reco step - Will have two output modules. Filter name can be anything,
        data tiers will be "GEN-SIM-RECO" and "ALCARECO".
        
        """
        config = self._getConfigBase()
        config["pset_tweak_details"] = \
            {"process": {"outputModules_": ["RecoA", "RecoB"],
                         "RecoA": {"dataset": {"filterName": "RecoAFilter",
                                               "dataTier": "GEN-SIM-RECO"}},
                         "RecoB": {"dataset": {"filterName": "RecoBFilter",
                                               "dataTier": "ALCARECO"}}}}
        result = self.configDatabase.commitOne(config)
        return result[0]["id"]
    
    
    def injectAlcaRecoConfig(self):
        """
        AlcaReco step configuration, will have two output modules.
        Filter name can be anything, data tiers can be anything.
        
        """
        config = self._getConfigBase()
        config["pset_tweak_details"] = \
            {"process": {"outputModules_": ["AlcaRecoA", "AlcaRecoB"],
                         "AlcaRecoA": {"dataset": {"filterName": "AlcaRecoAFilter",
                                                   "dataTier": "GEN-SIM-RECO-ALCARECOA"}},
                         "AlcaRecoB": {"dataset": {"filterName": "AlcaRecoBFilter",
                                                   "dataTier": "GEN-SIM-RECO-ALCARECOB"}}}}
        result = self.configDatabase.commitOne(config)
        return result[0]["id"]


    def _generationTaskTest(self):
        # retrieve task from the installed workflow
        genTask = Workflow(name = "TestWorkload", task = "/TestWorkload/Generation")
        genTask.load()
        self.assertEqual(len(genTask.outputMap.keys()), 2, "Error: Wrong number of WF outputs.")
        
        # output modules
        goldenOutputMods = ["OutputA"]
        
        for o in goldenOutputMods:
            mergedOutput = genTask.outputMap[o]["merged_output_fileset"]
            unmergedOutput = genTask.outputMap[o]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()
            self.assertEqual(mergedOutput.name, "/TestWorkload/Generation/GenerationMerge%s/merged-Merged" % o,
                             "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
            self.assertEqual(unmergedOutput.name, "/TestWorkload/Generation/unmerged-%s" % o,
                             "Error: Unmerged output fileset is wrong.")
            
        logArchOutput = genTask.outputMap["logArchive"]["merged_output_fileset"]
        unmergedLogArchOutput = genTask.outputMap["logArchive"]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()
        self.assertEqual(logArchOutput.name, "/TestWorkload/Generation/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")
        self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Generation/unmerged-logArchive",
                         "Error: LogArchive output fileset is wrong.")

        for o in goldenOutputMods:
            mergeTask = Workflow(name = "TestWorkload",
                                 task = "/TestWorkload/Generation/GenerationMerge%s" % o)
            mergeTask.load()
            self.assertEqual(len(mergeTask.outputMap.keys()), 2, "Error: Wrong number of WF outputs.")
            mergedMergeOutput = mergeTask.outputMap["Merged"]["merged_output_fileset"]
            unmergedMergeOutput = mergeTask.outputMap["Merged"]["output_fileset"]
            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()
            self.assertEqual(mergedMergeOutput.name, "/TestWorkload/Generation/GenerationMerge%s/merged-Merged" % o,
                             "Error: Merged output fileset is wrong.")
            self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/Generation/GenerationMerge%s/merged-Merged" % o,
                             "Error: Unmerged output fileset is wrong.")
            logArchOutput = mergeTask.outputMap["logArchive"]["merged_output_fileset"]
            unmergedLogArchOutput = mergeTask.outputMap["logArchive"]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()
            self.assertEqual(logArchOutput.name, "/TestWorkload/Generation/GenerationMerge%s/merged-logArchive" % o,
                             "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/Generation/GenerationMerge%s/merged-logArchive" % o,
                             "Error: LogArchive output fileset is wrong.")
            
        topLevelFileset = Fileset(name = "TestWorkload-Generation-SomeBlock")
        topLevelFileset.loadData()
        prodSubscription = Subscription(fileset = topLevelFileset, workflow = genTask)
        prodSubscription.loadData()
        self.assertEqual(prodSubscription["type"], "Production", "Error: Wrong subscription type.")
        self.assertEqual(prodSubscription["split_algo"], "EventBased", "Error: Wrong split algo.")

        for o in goldenOutputMods:
            unmergedOutput = Fileset(name = "/TestWorkload/Generation/unmerged-%s" % o)
            unmergedOutput.loadData()
            mergeTask = Workflow(name = "TestWorkload",
                                 task = "/TestWorkload/Generation/GenerationMerge%s" % o)
            mergeTask.load()
            mergeSubscription = Subscription(fileset = unmergedOutput, workflow = mergeTask)
            mergeSubscription.loadData()
            self.assertEqual(mergeSubscription["type"], "Merge", "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo: %s" % mergeSubscription["split_algo"])
            
        for o in goldenOutputMods:
            unmerged = Fileset(name = "/TestWorkload/Generation/unmerged-%s" % o)
            unmerged.loadData()
            cleanupTask = Workflow(name = "TestWorkload",
                                   task = "/TestWorkload/Generation/GenerationCleanupUnmerged%s" % o)
            cleanupTask.load()
            cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupTask)
            cleanupSubscription.loadData()
            self.assertEqual(cleanupSubscription["type"], "Cleanup", "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                             "Error: Wrong split algo.")
            
        genLogCollect = Fileset(name = "/TestWorkload/Generation/unmerged-logArchive")
        genLogCollect.loadData()
        genLogCollectTask = Workflow(name = "TestWorkload",
                                     task = "/TestWorkload/Generation/GenLogCollect")
        genLogCollectTask.load()
        logCollectSub = Subscription(fileset = genLogCollect, workflow = genLogCollectTask)
        logCollectSub.loadData()
        self.assertEqual(logCollectSub["type"], "LogCollect", "Error: Wrong subscription type.")
        self.assertEqual(logCollectSub["split_algo"], "EndOfRun", "Error: Wrong split algo.")

        for o in goldenOutputMods:
            mergeLogCollect = Fileset(name = "/TestWorkload/Generation/GenerationMerge%s/merged-logArchive" % o)
            mergeLogCollect.loadData()
            mergeLogCollectTask = Workflow(name = "TestWorkload",
                                           task = "/TestWorkload/Generation/GenerationMerge%s/Generation%sMergeLogCollect" % (o, o))
            mergeLogCollectTask.load()
            logCollectSub = Subscription(fileset = mergeLogCollect, workflow = mergeLogCollectTask)
            logCollectSub.loadData()
            self.assertEqual(logCollectSub["type"], "LogCollect", "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "EndOfRun", "Error: Wrong split algo.")

        
    def _reconstructionTaskTest(self):
        recoTask = Workflow(name = "TestWorkload",
                            task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction")        
        recoTask.load()
        self.assertEqual(len(recoTask.outputMap.keys()), 3, "Error: Wrong number of WF outputs.")
        
        # output modules
        goldenOutputMods = ["RecoA", "RecoB"]
        
        for o in goldenOutputMods:
            mergedOutput = recoTask.outputMap[o]["merged_output_fileset"]
            unmergedOutput = recoTask.outputMap[o]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()
            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMerge%s/merged-Merged" % o,
                             ("Error: Merged output fileset is wrong: %s" % mergedOutput.name))
            self.assertEqual(unmergedOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/unmerged-%s" % o,
                             ("Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name))
            
        logArchOutput = recoTask.outputMap["logArchive"]["merged_output_fileset"]
        unmergedLogArchOutput = recoTask.outputMap["logArchive"]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()
        self.assertEqual(logArchOutput.name,
                         "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/unmerged-logArchive",
                         ("Error: LogArchive output fileset is wrong: %s" % logArchOutput.name))
        self.assertEqual(unmergedLogArchOutput.name,
                         "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/unmerged-logArchive",
                         ("Error: LogArchive output fileset is wrong: %s" % unmergedLogArchOutput.name))

        for o in goldenOutputMods:
            mergeTask = Workflow(name = "TestWorkload",
                                 task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMerge%s" % o)
            mergeTask.load()
            self.assertEqual(len(mergeTask.outputMap.keys()), 2, "Error: Wrong number of WF outputs.")
            mergedMergeOutput = mergeTask.outputMap["Merged"]["merged_output_fileset"]
            unmergedMergeOutput = mergeTask.outputMap["Merged"]["output_fileset"]
            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()
            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMerge%s/merged-Merged" % o,
                             ("Error: Merged output fileset is wrong: %s" % mergedMergeOutput.name))
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMerge%s/merged-Merged" % o,
                             ("Error: Unmerged output fileset is wrong: %s" % unmergedMergeOutput.name))
            logArchOutput = mergeTask.outputMap["logArchive"]["merged_output_fileset"]
            unmergedLogArchOutput = mergeTask.outputMap["logArchive"]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()
            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMerge%s/merged-logArchive" % o,
                             ("Error: LogArchive output fileset is wrong: %s" % logArchOutput.name))
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMerge%s/merged-logArchive" % o,
                             ("Error: LogArchive output fileset is wrong: %s" % unmergedLogArchOutput.name))

        for o in goldenOutputMods:
            unmerged = Fileset(name = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/unmerged-%s" % o)
            unmerged.loadData()
            
            mergeTask = Workflow(name = "TestWorkload",                    
                                      task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMerge%s" % o)
            mergeTask.load()
            mergeSubscription = Subscription(fileset = unmerged, workflow = mergeTask)
            mergeSubscription.loadData()            
            self.assertEqual(mergeSubscription["type"], "Merge", "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize", "Error: Wrong split algo.")

        for o in goldenOutputMods:
            unmerged = Fileset(name = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/unmerged-%s" % o)
            unmerged.loadData()
            cleanupTask = Workflow(name = "TestWorkload",
                                   task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionCleanupUnmerged%s" % o)            
            cleanupTask.load()
            cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupTask)
            cleanupSubscription.loadData()
            self.assertEqual(cleanupSubscription["type"], "Cleanup", "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased", "Error: Wrong split algo.")
            
        for o in goldenOutputMods:
            recoMergeLogCollect = Fileset(name =
                                          "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMerge%s/merged-logArchive" % o)
            recoMergeLogCollect.loadData()
            recoMergeLogCollectTask = Workflow(name = "TestWorkload",
                                               task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMerge%s/Reconstruction%sMergeLogCollect" % (o, o))
            recoMergeLogCollectTask.load()
            logCollectSub = Subscription(fileset = recoMergeLogCollect, workflow = recoMergeLogCollectTask)
            logCollectSub.loadData()
            self.assertEqual(logCollectSub["type"], "LogCollect", "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "EndOfRun", "Error: Wrong split algo.")


    def _alcaRecoTaskTest(self):
        """
        Test for the final third task, resp. 5th task, ALCARECO. It runs on
        dataTier "GEN-SIM-RECO" output of the previous Reconstruction task
        so we only consider RecoA as output module of the Reconstruction task
        (and not RecoB).
        
        """
        alcaRecoTask = Workflow(name = "TestWorkload",
                                task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO")        
        alcaRecoTask.load()
        self.assertEqual(len(alcaRecoTask.outputMap.keys()), 3,
                         "Error: Wrong number of WF outputs.")
        
        # output modules
        goldenOutputMods = ["AlcaRecoA", "AlcaRecoB"]
        
        for o in goldenOutputMods:
            mergedOutput = alcaRecoTask.outputMap[o]["merged_output_fileset"]
            unmergedOutput = alcaRecoTask.outputMap[o]["output_fileset"]
            mergedOutput.loadData()
            unmergedOutput.loadData()
            self.assertEqual(mergedOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOMerge%s/merged-Merged" % o,
                             ("Error: Merged output fileset is wrong: %s" % mergedOutput.name))
            self.assertEqual(unmergedOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/unmerged-%s" % o,
                             ("Error: Unmerged output fileset is wrong: %s" % unmergedOutput.name))
            
        logArchOutput = alcaRecoTask.outputMap["logArchive"]["merged_output_fileset"]
        unmergedLogArchOutput = alcaRecoTask.outputMap["logArchive"]["output_fileset"]
        logArchOutput.loadData()
        unmergedLogArchOutput.loadData()
        self.assertEqual(logArchOutput.name,
                         "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/unmerged-logArchive",
                         ("Error: LogArchive output fileset is wrong: %s" % logArchOutput.name))
        self.assertEqual(unmergedLogArchOutput.name,
                         "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/unmerged-logArchive",
                         ("Error: LogArchive output fileset is wrong: %s" % unmergedLogArchOutput.name))

        for o in goldenOutputMods:
            mergeTask = Workflow(name = "TestWorkload",
                                 task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOMerge%s" % o)
            mergeTask.load()
            self.assertEqual(len(mergeTask.outputMap.keys()), 2, "Error: Wrong number of WF outputs.")
            mergedMergeOutput = mergeTask.outputMap["Merged"]["merged_output_fileset"]
            unmergedMergeOutput = mergeTask.outputMap["Merged"]["output_fileset"]
            mergedMergeOutput.loadData()
            unmergedMergeOutput.loadData()
            self.assertEqual(mergedMergeOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOMerge%s/merged-Merged" % o,                             
                             ("Error: Merged output fileset is wrong: %s" % mergedMergeOutput.name))
            self.assertEqual(unmergedMergeOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOMerge%s/merged-Merged" % o,                             
                             ("Error: Unmerged output fileset is wrong: %s" % unmergedMergeOutput.name))
            logArchOutput = mergeTask.outputMap["logArchive"]["merged_output_fileset"]
            unmergedLogArchOutput = mergeTask.outputMap["logArchive"]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()
            self.assertEqual(logArchOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOMerge%s/merged-logArchive" % o,                             
                             ("Error: LogArchive output fileset is wrong: %s" % logArchOutput.name))
            self.assertEqual(unmergedLogArchOutput.name,
                             "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOMerge%s/merged-logArchive" % o,                             
                             ("Error: LogArchive output fileset is wrong: %s" % unmergedLogArchOutput.name))

        for o in goldenOutputMods:
            unmerged = Fileset(name = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/unmerged-%s" % o)
            unmerged.loadData()            
            mergeTask = Workflow(name = "TestWorkload",
                                 task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOMerge%s" % o)
            mergeTask.load()
            mergeSubscription = Subscription(fileset = unmerged, workflow = mergeTask)
            mergeSubscription.loadData()            
            self.assertEqual(mergeSubscription["type"], "Merge", "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize", "Error: Wrong split algo.")
        
        for o in goldenOutputMods:
            unmerged = Fileset(name = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/unmerged-%s" % o)
            unmerged.loadData()
            cleanupTask = Workflow(name = "TestWorkload",
                                   task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOCleanupUnmerged%s" % o)                                    
            cleanupTask.load()
            cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupTask)
            cleanupSubscription.loadData()
            self.assertEqual(cleanupSubscription["type"], "Cleanup", "Error: Wrong subscription type.")
            self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased", "Error: Wrong split algo.")
        
        for o in goldenOutputMods:
            alcaRecoMergeLogCollect = Fileset(name = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOMerge%s/merged-logArchive" % o)
            alcaRecoMergeLogCollect.loadData()
            alcaRecoMergeLogCollectTask = Workflow(name = "TestWorkload",
                                                   task = "/TestWorkload/Generation/GenerationMergeOutputA/Reconstruction/ReconstructionMergeRecoA/ALCARECO/ALCARECOMerge%s/ALCARECO%sMergeLogCollect" % (o, o))                                               
            alcaRecoMergeLogCollectTask.load()
            logCollectSub = Subscription(fileset = alcaRecoMergeLogCollect, workflow = alcaRecoMergeLogCollectTask)
            logCollectSub.loadData()
            self.assertEqual(logCollectSub["type"], "LogCollect", "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "EndOfRun", "Error: Wrong split algo.")

    
    def testRelValMC(self):
        """
        Configure, instantiate, install into WMBS and check that the
        subscriptions in WMBS are setup correctly.
        
        """
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "rereco_t"        
        defaultArguments["GenConfigCacheID"] = self.injectGenerationConfig()
        defaultArguments["RecoConfigCacheID"] = self.injectReconstructionConfig()
        defaultArguments["AlcaRecoConfigCacheID"] = self.injectAlcaRecoConfig() 

        testWorkload = relValMCWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")
        
        testWMBSHelper = WMBSHelper(testWorkload, "SomeBlock")
        testWMBSHelper.createSubscription()
        
        # now run the tests on single workload instance installed into WMBS
        # each of the subtests is dealing with specific tasks
        self._generationTaskTest()
        self._reconstructionTaskTest()
        self._alcaRecoTaskTest()
        


if __name__ == "__main__":
    unittest.main()    