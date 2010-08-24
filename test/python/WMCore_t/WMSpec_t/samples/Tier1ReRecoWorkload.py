#!/usr/bin/env python
"""
_Tier1ReRecoWorkload_



"""
import os, pickle, sys, shutil
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper
from WMCore.WMSpec.StdSpecs.ReReco import rerecoWorkload




arguments = {
    "OutputTiers" : ['RECO', 'ALCARECO', 'AOD'],
    "AcquisitionEra" : "Teatime09",
    "GlobalTag" :"GR09_P_V7::All",
    "LFNCategory" : "/store/data",
    "ProcessingVersion" : "v99",
    "Scenario" : "cosmics",
    "CMSSWVersion" : "CMSSW_3_3_5_patch3",
    "InputDatasets" : "/MinimumBias/BeamCommissioning09-v1/RAW",
    "Emulate" : True,
    }

workload = rerecoWorkload("Tier1ReReco", arguments)
rereco = workload.getTask("ReReco")



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

import random


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





def testMergeSubscription(unmergedOutMod, mergeTask):
    """
    _testMergeSubscription_
    
    Generate a subscription containing a few faked up files for a merge job
    
    """
    fileset = Fileset(name = "%s-MergeFiles" % mergeTask.getPathName())
    lfnBase = unmergedOutMod.lfnBase
    for i in range(0, random.randint(15,25)):
        inpFile = File("%s/%s.root" %(lfnBase, makeUUID()),
                       random.randint(200000, 1000000),
                       random.randint(1000,2000)
        )
        fileset.addFile(inpFile)
    work = mergeTask.makeWorkflow()
    subscription = Subscription(
               fileset = fileset,
               workflow = work,
               split_algo = mergeTask.jobSplittingAlgorithm(),
               type = "Merge")
    return subscription
        

rerecoCmssw = rereco.getStep("cmsRun1")
rerecoCmsswHelper = rerecoCmssw.getTypeHelper()
mergeReco = workload.getTaskByPath("/Tier1ReReco/ReReco/MergeReco")
mergeAlca = workload.getTaskByPath("/Tier1ReReco/ReReco/MergeAlca")
mergeAod  = workload.getTaskByPath("/Tier1ReReco/ReReco/MergeAod")

if 'RECO' in arguments['OutputTiers']:
    mergeRecoSubs = testMergeSubscription( rerecoCmsswHelper.getOutputModule("outputRECORECO"), mergeReco)
if 'AOD' in arguments['OutputTiers']:
    mergeAodSubs = testMergeSubscription( rerecoCmsswHelper.getOutputModule("outputAODRECO"), mergeAod)
if 'ALCARECO' in arguments['OutputTiers']:
    mergeAlcaSubs = testMergeSubscription( rerecoCmsswHelper.getOutputModule("outputALCARECORECO"), mergeAlca)


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
    
def makeMergeJobs(task, subs):
    """
    _makeMergeJobs_
    
    
    """
    splitter = SplitterFactory()
    jobfactory = splitter(subs, "WMCore.DataStructs", makeGenerators(task))
    params = task.jobSplittingParameters()
    jobGroups = jobfactory(**params)
    print jobGroups
    package = JobPackage()
    [ package.extend(group.getJobs()) for group in jobGroups ]
    print package
    
    return package

if __name__ == '__main__':
    pkg = makeRerecoJobs(rereco, subs)
    saveRecoPkg = "%s/RecoJobPackage.pkl" % workingDir
    pkg.save(saveRecoPkg)

    if 'RECO' in arguments['OutputTiers']:
        mrgRecoPkg = makeMergeJobs(mergeReco, mergeRecoSubs)
        saveMergeRecoPkg = "%s/MergeRecoJobPackage.pkl" % workingDir
        mrgRecoPkg.save(saveMergeRecoPkg)
    if 'AOD' in arguments['OutputTiers']:
        mrgAodPkg = makeMergeJobs(mergeAod, mergeAodSubs)
        saveMergeAodPkg = "%s/MergeAodJobPackage.pkl" % workingDir
        mrgAodPkg.save(saveMergeAodPkg)
    if 'ALCARECO' in arguments['OutputTiers']:
        mrgAlcaPkg = makeMergeJobs(mergeAlca, mergeAlcaSubs)
        saveMergeAlcaPkg = "%s/MergeAlcaJobPackage.pkl" % workingDir
        mrgAlcaPkg.save(saveMergeAlcaPkg)



#  //
# // generate a convenience script to setup and run the job
#//
if __name__ == '__main__':
    import inspect

    import WMCore.WMRuntime as WMRuntime

    runtimeLoc = inspect.getsourcefile(WMRuntime)
    unpacker = runtimeLoc.replace('__init__.py', "Unpacker.py")
    sandbox = rereco.data.sandboxArchivePath

    rerecoCommand = "mkdir rereco; cd rereco; %s %s --sandbox=%s --package=%s --index=1; cd ..\n" % (
        sys.executable,
        unpacker,
        sandbox,
        saveRecoPkg,
    )
     
    mergerecoCommand = "mkdir mergereco; cd mergereco; %s %s --sandbox=%s --package=%s --index=1; cd ..\n" % (
        sys.executable,
        unpacker,
        mergeReco.data.sandboxArchivePath,
        saveMergeRecoPkg,
    )
    mergeaodCommand = "mkdir mergeaod; cd mergeaod; %s %s --sandbox=%s --package=%s --index=1; cd ..\n" % (
        sys.executable,
        unpacker,
        mergeAod.data.sandboxArchivePath,
        saveMergeAodPkg,
    )
    mergealcaCommand = "mkdir mergealca; cd mergealca; %s %s --sandbox=%s --package=%s --index=1; cd ..\n" % (
        sys.executable,
        unpacker,
        mergeAlca.data.sandboxArchivePath,
        saveMergeAlcaPkg,
    )
    
    

    runitScript = \
    """
    %s
    %s
    %s
    %s
    
    echo "#To start rereco job do:"
    echo "export PYTHONPATH=`pwd`/rereco/job"
    echo "cd rereco/job"
    echo "%s WMCore/WMRuntime/Startup.py"
    """ % (
        rerecoCommand,
        mergerecoCommand,
        mergeaodCommand,
        mergealcaCommand,
        sys.executable)

    runitFile = open("%s/build.sh" % workingDir, 'w')
    runitFile.write(runitScript)
    runitFile.close()


