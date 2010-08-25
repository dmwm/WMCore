#!/usr/bin/env python
"""
_Tier1ReRecoWorkload_



"""
import os, pickle, sys
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper


#  //
# // Set up the basic workload task and step structure
#//
workload = newWorkload("Tier1ReReco")
workload.setStartPolicy('DatasetBlock')
workload.setEndPolicy('SingleShot')

#  //
# // set up the production task
#//
rereco = workload.newTask("ReReco")
rerecoCmssw = rereco.makeStep("cmsRun1")
rerecoCmssw.setStepType("CMSSW")
#skimStageOut = rerecoCmssw.addStep("stageOut1")
#skimStageOut.setStepType("StageOut")
rereco.applyTemplates()
rereco.setSplittingAlgorithm("FileBased", files_per_job = 1)
rereco.addInputDataset(
    primary = "Cosmics",
    processed = "CRAFT09-PromptReco-v1",
    tier = "RECO",
    dbsurl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")



#  //
# // rereco cmssw step
#//
#
# TODO: Anywhere helper.data is accessed means we need a method added to the
# type based helper class to provide a clear API.
rerecoCmsswHelper = rerecoCmssw.getTypeHelper()


rerecoCmsswHelper.cmsswSetup(
    "CMSSW_3_1_2",
    softwareEnvironment = " . /uscmst1/prod/sw/cms/bashrc prod"
    )

rerecoCmsswHelper.setDataProcessingConfig(
    "cosmics", "promptReco", globalTag = "GLOBAL::BALLS",
    writeTiers = ['RECO'])

rerecoCmsswHelper.addOutputModule(
    "outputRECO", primaryDataset = "Primary",
    processedDataset = "Processed",
    dataTier = "RECO")


rereco.addGenerator("BasicNaming")



#  //TODO
# // rereco stage out step
#//
#skimStageOutHelper =skimStageOut.getTypeHelper()


#  //
# // TODO: Add merge for ReReco outputs
#//


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
    dbs = DbsApi({"url" : "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"})


    fileset1 = Fileset(name = 'SkimFiles')


    for dbsF in dbs.listFiles(patternLFN = "/store/data/CRAFT09/Cosmics/RECO/v1/000/108/483/*", retriveList = ['retrive_lumi', 'retrive_run']):

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
    print work.task
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


