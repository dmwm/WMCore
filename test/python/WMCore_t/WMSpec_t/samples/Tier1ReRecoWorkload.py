#!/usr/bin/env python
"""
_Tier1ReRecoWorkload_



"""
import os, pickle, sys, shutil
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper


#  //
# // Arguments along the lines 
#//
writeDataTiers = ['RECO', 'ALCA', 'AOD']
acquisitionEra = "Teatime09"
globalTagSetting = "GR09_P_V7::All"
lfnCategory = "/store/data"
tempLfnCategory = "/store/unmerged"
processingVersion = "v99"
scenario = "cosmics"
cmsswVersion = "CMSSW_3_3_5_patch3"
softwareInitCommand = " . /uscmst1/prod/sw/cms/shrc prod"
scramArchitecture = "slc5_ia32_gcc434",

inputPrimaryDataset = "MinimumBias"
inputProcessedDataset = "BeamCommissioning09-v1"
inputDataTier = "RAW"

emulationMode = True

#  //
# // Set up the basic workload task and step structure
#//
workload = newWorkload("Tier1ReReco")
workload.setStartPolicy('DatasetBlock')
workload.setEndPolicy('SingleShot')
workload.data.properties.acquisitionEra = acquisitionEra


#  //
# // set up the production task
#//
rereco = workload.newTask("ReReco")
rerecoCmssw = rereco.makeStep("cmsRun1")
rerecoCmssw.setStepType("CMSSW")
rerecoStageOut = rerecoCmssw.addStep("stageOut1")
rerecoStageOut.setStepType("StageOut")
rerecoLogArch = rerecoCmssw.addStep("logArch1")
rerecoLogArch.setStepType("LogArchive")
rereco.applyTemplates()
rereco.setSplittingAlgorithm("FileBased", files_per_job = 1)
rereco.addGenerator("BasicNaming")
rereco.addGenerator("BasicCounter")
rereco.setTaskType("Processing")



#  //
# // rereco cmssw step
#//
#
# TODO: Anywhere helper.data is accessed means we need a method added to the
# type based helper class to provide a clear API.
rerecoCmsswHelper = rerecoCmssw.getTypeHelper()
rereco.addInputDataset(
    primary = inputPrimaryDataset,
    processed = inputProcessedDataset,
    tier = inputDataTier,
    dbsurl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
    )

rerecoCmsswHelper.cmsswSetup(
    cmsswVersion,
    softwareEnvironment = softwareInitCommand ,
    scramArch = scramArchitecture
    )

rerecoCmsswHelper.setDataProcessingConfig(
    scenario, "promptReco", globalTag = globalTagSetting,
    writeTiers = writeDataTiers)


processedDatasetName = "rereco_%s_%s" % (globalTagSetting.replace("::","_"), processingVersion)
unmergedDatasetName = "%s-unmerged" % processedDatasetName
commonLfnBase = lfnCategory
commonLfnBase += "/%s" % acquisitionEra
commonLfnBase += "/%s" % inputPrimaryDataset
unmergedLfnBase = tempLfnCategory
unmergedLfnBase += "/%s" % acquisitionEra
unmergedLfnBase += "/%s" % inputPrimaryDataset


if "RECO" in writeDataTiers:
    rerecoCmsswHelper.addOutputModule(
        "outputRECO", primaryDataset = inputPrimaryDataset,
        processedDataset = unmergedDatasetName,
        dataTier = "RECO",
        lfnBase = "%s/RECO/%s" % ( unmergedLfnBase, processedDatasetName)
    )   
    
if "ALCA" in writeDataTiers:
    rerecoCmsswHelper.addOutputModule(
        "outputALCA", primaryDataset = inputPrimaryDataset,
        processedDataset = unmergedDatasetName,
        dataTier = "ALCA",
        lfnBase = "%s/ALCA/%s" % ( unmergedLfnBase, processedDatasetName)
    )  

if "AOD" in writeDataTiers:
    rerecoCmsswHelper.addOutputModule(
        "outputAOD", primaryDataset = inputPrimaryDataset,
        processedDataset = unmergedDatasetName,
        dataTier = "AOD",
        lfnBase = "%s/AOD/%s" % ( unmergedLfnBase, processedDatasetName)
    )  
    
                               

# manipulate stage out and log archive if needed via type specific helper
rerecoStageOutHelper = rerecoStageOut.getTypeHelper()
rerecoLogArchHelper  = rerecoLogArch.getTypeHelper()

# Emulation
if emulationMode:
    rerecoCmsswHelper.data.emulator.emulatorName = "CMSSW"
    rerecoStageOutHelper.data.emulator.emulatorName = "StageOut"
    rerecoLogArchHelper.data.emulator.emulatorName = "LogArchive"



#  //
# // Merges for each output module
#//
if "RECO" in writeDataTiers:
    mergeReco = rereco.addTask("MergeReco")
    mergeRecoCmssw = mergeReco.makeStep("mergeReco")    
    mergeRecoCmssw.setStepType("CMSSW")
    mergeRecoStageOut = mergeRecoCmssw.addStep("stageOut1")
    mergeRecoStageOut.setStepType("StageOut")
    mergeRecoLogArch = mergeRecoCmssw.addStep("logArch1")
    mergeRecoLogArch.setStepType("LogArchive")

    mergeReco.applyTemplates()
    mergeReco.setSplittingAlgorithm("MergeBySize", merge_size = 20000000)  
    mergeReco.addGenerator("BasicNaming")
    mergeReco.addGenerator("BasicCounter")
    mergeReco.setTaskType("Merge")
    mergeRecoCmsswHelper = mergeRecoCmssw.getTypeHelper()
    mergeRecoCmsswHelper.cmsswSetup(
        cmsswVersion,
        softwareEnvironment = softwareInitCommand,
        scramArch = scramArchitecture,
    )

    mergeRecoCmsswHelper.setDataProcessingConfig(scenario, "merge")
    mergeRecoCmsswHelper.addOutputModule(
        "Merged", primaryDataset = inputPrimaryDataset,
        processedDataset = processedDatasetName,
        dataTier = "RECO",
        lfnBase = "%s/RECO/%s" % ( commonLfnBase, processedDatasetName)
    )


    mergeReco.setInputReference(rerecoCmssw, outputModule = "outputRECO")
    if emulationMode:
        mergeRecoStageOutHelper = mergeRecoStageOut.getTypeHelper()
        mergeRecoLogArchHelper  = mergeRecoLogArch.getTypeHelper()
        mergeRecoCmsswHelper.data.emulator.emulatorName = "CMSSW"
        mergeRecoStageOutHelper.data.emulator.emulatorName = "StageOut"
        mergeRecoLogArchHelper.data.emulator.emulatorName = "LogArchive"

if "ALCA" in writeDataTiers:
    mergeAlca = rereco.addTask("MergeAlca")
    mergeAlcaCmssw = mergeAlca.makeStep("mergeAlca")    
    mergeAlcaCmssw.setStepType("CMSSW")
    mergeAlcaStageOut = mergeAlcaCmssw.addStep("stageOut1")
    mergeAlcaStageOut.setStepType("StageOut")
    mergeAlcaLogArch = mergeAlcaCmssw.addStep("logArch1")
    mergeAlcaLogArch.setStepType("LogArchive")
    mergeAlca.addGenerator("BasicNaming")
    mergeAlca.addGenerator("BasicCounter")
    mergeAlca.setTaskType("Merge")  
    mergeAlca.applyTemplates()
    mergeAlca.setSplittingAlgorithm("MergeBySize", merge_size = 20000000)  

    mergeAlcaCmsswHelper = mergeAlcaCmssw.getTypeHelper()
    mergeAlcaCmsswHelper.cmsswSetup(
        cmsswVersion,
        softwareEnvironment = softwareInitCommand,
        scramArch = scramArchitecture,
    )

    mergeAlcaCmsswHelper.setDataProcessingConfig(scenario, "merge")
    mergeAlcaCmsswHelper.addOutputModule(
        "Merged", primaryDataset = inputPrimaryDataset,
        processedDataset = processedDatasetName,
        dataTier = "ALCA",
        lfnBase = "%s/ALCA/%s" % ( commonLfnBase, processedDatasetName)
    )
    
    
    mergeAlca.setInputReference(rerecoCmssw, outputModule = "outputALCA")
    if emulationMode:
        mergeAlcaStageOutHelper = mergeAlcaStageOut.getTypeHelper()
        mergeAlcaLogArchHelper  = mergeAlcaLogArch.getTypeHelper()
        mergeAlcaCmsswHelper.data.emulator.emulatorName = "CMSSW"
        mergeAlcaStageOutHelper.data.emulator.emulatorName = "StageOut"
        mergeAlcaLogArchHelper.data.emulator.emulatorName = "LogArchive"
    

        

if "AOD" in writeDataTiers:
    mergeAod = rereco.addTask("MergeAod")
    mergeAodCmssw = mergeAod.makeStep("mergeAod")    
    mergeAodCmssw.setStepType("CMSSW")
    mergeAodStageOut = mergeAodCmssw.addStep("stageOut1")
    mergeAodStageOut.setStepType("StageOut")
    mergeAodLogArch = mergeAodCmssw.addStep("logArch1")
    mergeAodLogArch.setStepType("LogArchive")
    mergeAod.addGenerator("BasicNaming")
    mergeAod.addGenerator("BasicCounter")
    mergeAod.setTaskType("Merge")
    mergeAod.applyTemplates()
    mergeAod.setSplittingAlgorithm("MergeBySize", merge_size = 20000000)  

    mergeAodCmsswHelper = mergeAodCmssw.getTypeHelper()
    mergeAodCmsswHelper.cmsswSetup(
        cmsswVersion,
        softwareEnvironment = softwareInitCommand,
        scramArch = scramArchitecture,
    )

    mergeAodCmsswHelper.setDataProcessingConfig(scenario, "merge")
    mergeAodCmsswHelper.addOutputModule(
        "Merged", primaryDataset = inputPrimaryDataset,
        processedDataset = processedDatasetName,
        dataTier = "AOD",
        lfnBase = "%s/AOD/%s" % ( commonLfnBase, processedDatasetName)
    )
    
    mergeAod.setInputReference(rerecoCmssw, outputModule = "outputAOD")
    if emulationMode:
        mergeAodStageOutHelper = mergeAodStageOut.getTypeHelper()
        mergeAodLogArchHelper  = mergeAodLogArch.getTypeHelper()
        mergeAodCmsswHelper.data.emulator.emulatorName = "CMSSW"
        mergeAodStageOutHelper.data.emulator.emulatorName = "StageOut"
        mergeAodLogArchHelper.data.emulator.emulatorName = "LogArchive"
        



print workload.listAllTaskNames()







"""
Build the sandbox and sample jobs for the workload

Code below would largely run within the WMAgent itself, included here
to assist testing.


"""

from WMCore.WMSpec.Makers.TaskMaker import TaskMaker
from WMCore.WMFactory import WMFactory
from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from WMCore.DataStructs.Job import Job
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.JobPackage import JobPackage
from WMCore.DataStructs.JobGroup import JobGroup
from WMCore.Services.UUID import makeUUID

from WMCore.JobSplitting.Generators.GeneratorFactory import makeGenerators



workingDir = "%s/Tier1Workloads" % os.getcwd()
workloadDir = "%s/%s" % (workingDir, workload.name())

if not os.path.exists(workingDir):
    os.makedirs(workingDir)
if os.path.exists(workloadDir):
    os.system("rm -rf %s" % workloadDir)

#siteConfigPath = '%s/SITECONF/local/JobConfig/' %(workingDir)
#if not os.path.exists(siteConfigPath):
#    os.makedirs(siteConfigPath)
#shutil.copy('site-local-config.xml', siteConfigPath)
#environment = rereco.data.section_('environment')
#environment.CMS_PATH = workingDir

monitoring  = rereco.data.section_('watchdog')
monitoring.monitors = ['WMRuntimeMonitor', 'TestMonitor']
monitoring.section_('TestMonitor')
monitoring.TestMonitor.connectionURL = "dummy.cern.ch:99999/CMS"
monitoring.TestMonitor.password      = "ThisIsTheWorld'sStupidestPassword"
monitoring.TestMonitor.softTimeOut   = 10000
monitoring.TestMonitor.hardTimeOut   = 20000


taskMaker = TaskMaker(workload, workingDir)
taskMaker.skipSubscription = True
taskMaker.processWorkload()


rereco = workload.getTask("ReReco")








def testSubscription():
    """
    _testSubscription_

    Generate a WMBS test subscription containing a couple of files

    """
    from DBSAPI.dbsApi import DbsApi
    dbs = DbsApi({"url" : "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet", "version" : "DBS_2_0_8" })


    fileset1 = Fileset(name = 'SkimFiles')


    for dbsF in dbs.listFiles(patternLFN = "/store/data/BeamCommissioning09/MinimumBias/RAW/*", retriveList = ['retrive_lumi', 'retrive_run']):

        runs = {}
        for lumi in dbsF['LumiList']:
            runNum = lumi['RunNumber']
            if runNum not in runs.keys():
                runs[runNum] = Run(runNum)
            runs[runNum].lumis.append(lumi['LumiSectionNumber'])



        wmbsF = File(dbsF['LogicalFileName'],
                     dbsF['FileSize'],
                     dbsF['NumberOfEvents'])
        for run in runs.values():
            wmbsF.addRun(run)
        fileset1.addFile(wmbsF)


    work = rereco.makeWorkflow()
    subscription = Subscription(
        fileset = fileset1,
        workflow = work,
        split_algo = "FileBased",
        type = "ReReco")
    return subscription


if __name__ == '__main__':
    # keep test subscription local to speed things up
    pickledSubs = "%s/recosubscription.pkl" % workingDir
    if os.path.exists(pickledSubs):
        handle = open(pickledSubs, 'r')
        subs = pickle.load(handle)
        handle.close()
    else:
        subs = testSubscription()
        handle = open(pickledSubs, 'w')
        pickle.dump(subs, handle)
        handle.close()






def makeRerecoJobs(task, subscriptionWithFiles):
    """
    _makeRerecoJobs_

    Generate some rereco jobs from a subscription

    """
    splitter = SplitterFactory()
    jobfactory = splitter(subscriptionWithFiles, "WMCore.DataStructs", makeGenerators(task))

    jobGroups = jobfactory(files_per_job = 1)

    

    package = JobPackage()
    [ package.extend(group.getJobs()) for group in jobGroups ]

    return package

if __name__ == '__main__':
    pkg = makeRerecoJobs(rereco, subs)
    savePkg = "%s/RecoJobPackage.pkl" % workingDir
    pkg.save(savePkg)


#  //
# // generate a convenience script to setup and run the job
#//
if __name__ == '__main__':
    import inspect

    import WMCore.WMRuntime as WMRuntime

    runtimeLoc = inspect.getsourcefile(WMRuntime)
    unpacker = runtimeLoc.replace('__init__.py', "Unpacker.py")
    sandbox = rereco.data.sandboxArchivePath

    runitScript = \
    """
    %s %s --sandbox=%s --package=%s --index=1
    echo "#To start job do:"
    echo "export PYTHONPATH=`pwd`/job"
    echo "cd job"
    echo "%s WMCore/WMRuntime/Startup.py"
    """ % (
        sys.executable,
        unpacker,
        sandbox,
        savePkg,
        sys.executable)

    runitFile = open("%s/build.sh" % workingDir, 'w')
    runitFile.write(runitScript)
    runitFile.close()


