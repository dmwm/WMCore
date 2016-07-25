#!/usr/bin/env python
"""
_WMBSHelper_

Use WMSpecParser to extract information for creating workflow, fileset, and subscription
"""

import logging
import threading
import traceback
from collections import defaultdict

from WMCore.WMRuntime.SandboxCreator import SandboxCreator
from WMCore.WMBS.File import File
from WMCore.DataStructs.File import File as DatastructFile
from WMCore.DataStructs.LumiList import LumiList
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Job import Job
from WMCore.WMException import WMException
from WMCore.DataStructs.Run import Run
from WMCore.DAOFactory import DAOFactory
from WMCore.WMConnectionBase import WMConnectionBase
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.BossAir.BossAirAPI import (BossAirAPI, BossAirException)
from WMCore.ResourceControl.ResourceControl import ResourceControl
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMComponent.DBS3Buffer.DBSBufferDataset import DBSBufferDataset

def wmbsSubscriptionStatus(logger, dbi, conn, transaction):
    """Function to return status of wmbs subscriptions
    """
    action = DAOFactory(package='WMBS',
                        logger=logger,
                        dbinterface=dbi)('Monitoring.SubscriptionStatus')
    return action.execute(conn=conn,
                          transaction=transaction)


class WorkQueueWMBSException(WMException):
    """
    Dummy exception class for exceptions raised
    in WMBS Helper.

    TODO: Do something useful

    """

    pass


def killWorkflow(workflowName, jobCouchConfig, bossAirConfig=None):
    """
    _killWorkflow_

    Kill a workflow that is already executing inside the agent.  This will
    mark all incomplete jobs as failed and files that belong to all
    non-cleanup and non-logcollect subscriptions as failed.  The name of the
    JSM couch database and the URL to the database must be passed in as well
    so the state transitions are logged.
    """
    myThread = threading.currentThread()
    daoFactory = DAOFactory(package="WMCore.WMBS",
                            logger=myThread.logger,
                            dbinterface=myThread.dbi)
    killFilesAction = daoFactory(classname="Subscriptions.KillWorkflow")
    killJobsAction = daoFactory(classname="Jobs.KillWorkflow")

    killFilesAction.execute(workflowName=workflowName,
                            conn=myThread.transaction.conn)

    liveJobs = killJobsAction.execute(workflowName=workflowName,
                                      conn=myThread.transaction.conn)

    changeState = ChangeState(jobCouchConfig)

    # Deal with any jobs that are running in the batch system
    # only works if we can start the API
    if bossAirConfig:
        bossAir = BossAirAPI(config=bossAirConfig, noSetup=True)
        killableJobs = []
        for liveJob in liveJobs:
            if liveJob["state"].lower() == 'executing':
                # Then we need to kill this on the batch system
                liveWMBSJob = Job(id=liveJob["id"])
                liveWMBSJob.update(liveJob)
                killableJobs.append(liveJob)
        # Now kill them
        try:
            logging.info("Killing %d jobs for workflow: %s", len(killableJobs), workflowName)
            bossAir.kill(jobs=killableJobs, workflowName=workflowName)
        except BossAirException as ex:
            # Something's gone wrong. Jobs not killed!
            logging.error("Error while trying to kill running jobs in workflow!\n")
            logging.error(str(ex))
            trace = getattr(ex, 'traceback', '')
            logging.error(trace)
            # But continue; we need to kill the jobs in the master
            # the batch system will have to take care of itself.

    liveWMBSJobs = defaultdict(list)
    for liveJob in liveJobs:
        if liveJob["state"] == "killed":
            # Then we've killed it already
            continue
        liveWMBSJob = Job(id=liveJob["id"])
        liveWMBSJob.update(liveJob)
        liveWMBSJobs[liveJob["state"]].append(liveWMBSJob)

    for state, jobsByState in liveWMBSJobs.items():
        if len(jobsByState) > 100 and state != "executing":
            # if there are to many jobs skip the couch and dashboard update
            # TODO: couch and dashboard need to be updated or parallel.
            changeState.check("killed", state)
            changeState.persist(jobsByState, "killed", state)
        else:
            changeState.propagate(jobsByState, "killed", state)
    return


def freeSlots(multiplier=1.0, minusRunning=False, allowedStates=['Normal'], knownCmsSites=None):
    """
    Get free resources from wmbs.

    Specify multiplier to apply a ratio to the actual numbers.
    minusRunning control if running jobs should be counted
    """
    rc_sites = ResourceControl().listThresholdsForCreate()
    thresholds = defaultdict(lambda: 0)
    jobCounts = defaultdict(dict)
    for name, site in rc_sites.items():
        if not site.get('cms_name'):
            logging.warning("Not fetching work for %s, cms_name not defined", name)
            continue
        if knownCmsSites and site['cms_name'] not in knownCmsSites:
            logging.warning(
                "%s doesn't appear to be a known cms site, work may fail to be acquired for it", site['cms_name'])
        if site['state'] not in allowedStates:
            continue
        slots = site['total_slots']
        thresholds[site['cms_name']] += (slots * multiplier)
        if minusRunning:
            jobCounts[site['cms_name']] = dict(
                (k, jobCounts[site['cms_name']].get(k, 0) + site['pending_jobs'].get(k, 0))
                for k in site['pending_jobs'])

    return dict(thresholds), dict(jobCounts)


class WMBSHelper(WMConnectionBase):
    """
    _WMBSHelper_

    Interface between the WorkQueue and WMBS.
    """

    def __init__(self, wmSpec, taskName, blockName=None, mask=None, cachepath='.'):
        """
        _init_

        Initialize DAOs and other things needed.
        """
        self.block = blockName
        self.mask = mask
        self.wmSpec = wmSpec
        self.topLevelTask = wmSpec.getTask(taskName)
        self.cachepath = cachepath
        self.isDBS = True

        self.topLevelFileset = None
        self.topLevelSubscription = None
        self.topLevelTaskDBSBufferId = None

        self.mergeOutputMapping = {}

        # Initiate the pieces you need to run your own DAOs
        WMConnectionBase.__init__(self, "WMCore.WMBS")
        myThread = threading.currentThread()
        self.dbsDaoFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                        logger=myThread.logger,
                                        dbinterface=myThread.dbi)

        # DAOs from WMBS for file commit
        self.setParentage = self.daofactory(classname="Files.SetParentage")
        self.setFileRunLumi = self.daofactory(classname="Files.AddRunLumi")
        self.setFileLocation = self.daofactory(classname="Files.SetLocationForWorkQueue")
        self.setFileAddChecksum = self.daofactory(classname="Files.AddChecksumByLFN")
        self.addFileAction = self.daofactory(classname="Files.Add")
        self.addToFileset = self.daofactory(classname="Files.AddDupsToFileset")
        self.getLocations = self.daofactory(classname="Locations.ListSites")
        self.getLocationInfo = self.daofactory(classname="Locations.GetSiteInfo")

        # DAOs from DBSBuffer
        self.dbsCreateFiles = self.dbsDaoFactory(classname="DBSBufferFiles.Add")
        self.dbsSetLocation = self.dbsDaoFactory(classname="DBSBufferFiles.SetLocationByLFN")
        self.dbsInsertLocation = self.dbsDaoFactory(classname="DBSBufferFiles.AddLocation")
        self.dbsSetChecksum = self.dbsDaoFactory(classname="DBSBufferFiles.AddChecksumByLFN")
        self.dbsInsertWorkflow = self.dbsDaoFactory(classname="InsertWorkflow")

        # Added for file creation bookkeeping
        self.dbsFilesToCreate = []
        self.addedLocations = []
        self.wmbsFilesToCreate = []
        self.insertedBogusDataset = -1

        return

    def createSandbox(self):
        """Create the runtime sandbox"""
        sandboxCreator = SandboxCreator()
        sandboxCreator.makeSandbox(self.cachepath, self.wmSpec)

    def createTopLevelFileset(self, topLevelFilesetName=None):
        """
        _createTopLevelFileset_

        Create the top level fileset for the workflow.  If the name of the top
        level fileset is not given create one.
        """
        if topLevelFilesetName is None:
            filesetName = ("%s-%s" % (self.wmSpec.name(),
                                      self.wmSpec.getTopLevelTask()[0].name()))
            if self.block:
                filesetName += "-%s" % self.block
            if self.mask:
                from hashlib import md5
                mask_string = ",".join(["%s=%s" % (x, self.mask[x]) for x in sorted(self.mask)])
                filesetName += "-%s" % md5(mask_string).hexdigest()
        else:
            filesetName = topLevelFilesetName

        self.topLevelFileset = Fileset(filesetName)
        self.topLevelFileset.create()
        return

    def outputFilesetName(self, task, outputModuleName):
        """
        _outputFilesetName_

        Generate an output fileset name for the given task and output module.
        """
        if task.taskType() == "Merge":
            outputFilesetName = "%s/merged-%s" % (task.getPathName(),
                                                  outputModuleName)
        else:
            outputFilesetName = "%s/unmerged-%s" % (task.getPathName(),
                                                    outputModuleName)

        return outputFilesetName

    def createSubscription(self, task, fileset, alternativeFilesetClose=False):
        """
        _createSubscription_

        Create subscriptions in the database.
        This includes workflows in WMBS and DBSBuffer, output maps, datasets
        and phedex subscriptions, and filesets for each task below and including
        the given task.
        """
        sub = self._createSubscriptionsInWMBS(task, fileset, alternativeFilesetClose)

        self._createWorkflowsInDBSBuffer()
        self._createDatasetSubscriptionsInDBSBuffer()

        return sub

    def _createSubscriptionsInWMBS(self, task, fileset, alternativeFilesetClose=False):
        """
        __createSubscriptionsInWMBS_

        Create subscriptions in WMBS for all the tasks in the spec.  This
        includes filesets, workflows and the output map for each task.
        """
        # create runtime sandbox for workflow
        self.createSandbox()

        # FIXME: Let workflow put in values if spec is missing them
        workflow = Workflow(spec=self.wmSpec.specUrl(), owner=self.wmSpec.getOwner()["name"],
                            dn=self.wmSpec.getOwner().get("dn", "unknown"),
                            group=self.wmSpec.getOwner().get("group", "unknown"),
                            owner_vogroup=self.wmSpec.getOwner().get("vogroup", "DEFAULT"),
                            owner_vorole=self.wmSpec.getOwner().get("vorole", "DEFAULT"),
                            name=self.wmSpec.name(), task=task.getPathName(),
                            wfType=self.wmSpec.getDashboardActivity(),
                            alternativeFilesetClose=alternativeFilesetClose,
                            priority=self.wmSpec.priority())
        workflow.create()
        subscription = Subscription(fileset=fileset, workflow=workflow,
                                    split_algo=task.jobSplittingAlgorithm(),
                                    type=task.getPrimarySubType())
        if subscription.exists():
            subscription.load()
            msg = "Subscription %s already exists for %s (you may ignore file insertion messages below, existing files wont be duplicated)"
            self.logger.info(msg % (subscription['id'], task.getPathName()))
        else:
            subscription.create()
        for site in task.siteWhitelist():
            subscription.addWhiteBlackList([{"site_name": site, "valid": True}])

        for site in task.siteBlacklist():
            subscription.addWhiteBlackList([{"site_name": site, "valid": False}])

        if self.topLevelSubscription is None:
            self.topLevelSubscription = subscription
            logging.info("Top level subscription created: %s", subscription["id"])
        else:
            logging.info("Child subscription created: %s", subscription["id"])

        outputModules = task.getOutputModulesForTask()
        ignoredOutputModules = task.getIgnoredOutputModulesForTask()
        for outputModule in outputModules:
            for outputModuleName in outputModule.listSections_():
                if outputModuleName in ignoredOutputModules:
                    logging.info("IgnoredOutputModule set for %s, skipping fileset creation.", outputModuleName)
                    continue
                outputFileset = Fileset(self.outputFilesetName(task, outputModuleName))
                outputFileset.create()
                outputFileset.markOpen(True)
                mergedOutputFileset = None

                for childTask in task.childTaskIterator():
                    if childTask.data.input.outputModule == outputModuleName:
                        if childTask.taskType() == "Merge":
                            mergedOutputFileset = Fileset(self.outputFilesetName(childTask, "Merged"))
                            mergedOutputFileset.create()
                            mergedOutputFileset.markOpen(True)

                            primaryDataset = getattr(getattr(outputModule, outputModuleName), "primaryDataset", None)
                            if primaryDataset != None:
                                self.mergeOutputMapping[mergedOutputFileset.id] = primaryDataset

                        self._createSubscriptionsInWMBS(childTask, outputFileset, alternativeFilesetClose)

                if mergedOutputFileset is None:
                    workflow.addOutput(outputModuleName, outputFileset,
                                       outputFileset)
                else:
                    workflow.addOutput(outputModuleName, outputFileset,
                                       mergedOutputFileset)

        return self.topLevelSubscription

    def addMCFakeFile(self):
        """Add a fake file for wmbs to run production over"""
        needed = ['FirstEvent', 'FirstLumi', 'FirstRun', 'LastEvent', 'LastLumi', 'LastRun']
        for key in needed:
            if self.mask and self.mask.get(key) is None:
                msg = 'Invalid value "%s" for %s' % (self.mask.get(key), key)
                raise WorkQueueWMBSException(msg)
        locations = set()
        for site in self.getLocations.execute(conn=self.getDBConn(),
                                              transaction=self.existingTransaction()):
            try:
                siteInfo = self.getLocationInfo.execute(site, conn=self.getDBConn(),
                                                        transaction=self.existingTransaction())
                if not siteInfo:
                    self.logger.info('Skipping MonteCarlo injection to site "%s" as unknown to wmbs' % site)
                    continue
                locations.add(siteInfo[0]['pnn'])
            except Exception as ex:
                self.logger.error('Error getting storage element for "%s": %s' % (site, str(ex)))
        if not locations:
            msg = 'No locations to inject Monte Carlo work to, unable to proceed'
            raise WorkQueueWMBSException(msg)
        mcFakeFileName = ("MCFakeFile-%s" % self.topLevelFileset.name).encode('ascii', 'ignore')
        wmbsFile = File(lfn=mcFakeFileName,
                        first_event=self.mask['FirstEvent'],
                        last_event=self.mask['LastEvent'],
                        events=self.mask['LastEvent'] - self.mask['FirstEvent'] + 1,  # inclusive range
                        locations=locations,
                        merged=False,  # merged causes dbs parentage relation
                       )

        if self.mask:
            lumis = range(self.mask['FirstLumi'], self.mask['LastLumi'] + 1)  # inclusive range
            wmbsFile.addRun(Run(self.mask['FirstRun'], *lumis))  # assume run number static
        else:
            wmbsFile.addRun(Run(1, 1))

        wmbsFile['inFileset'] = True  # file is not a parent

        logging.info("WMBS File: %s on Location: %s", wmbsFile['lfn'], wmbsFile['newlocations'])

        self.wmbsFilesToCreate.append(wmbsFile)

        totalFiles = self.topLevelFileset.addFilesToWMBSInBulk(self.wmbsFilesToCreate,
                                                               self.wmSpec.name(),
                                                               isDBS=self.isDBS)

        self.topLevelFileset.markOpen(False)
        return totalFiles

    def createSubscriptionAndAddFiles(self, block):
        """
        _createSubscriptionAndAddFiles_

        Create the subscription and add files at one time to
        put everything in one transaction.

        """
        self.beginTransaction()

        self.createTopLevelFileset()
        try:
            sub = self.createSubscription(self.topLevelTask, self.topLevelFileset)
        except Exception as ex:
            myThread = threading.currentThread()
            myThread.transaction.rollback()
            msg = traceback.format_exc()
            logging.error("Failed to create subscription %s", msg)
            raise ex

        if block != None:
            logging.info('"%s" Injecting block %s (%d files) into wmbs', self.wmSpec.name(),
                                                                         self.block,
                                                                         len(block['Files']))
            addedFiles = self.addFiles(block)
        # For MC case
        else:
            logging.info(
                '"%s" Injecting production %s:%s:%s - %s:%s:%s (run:lumi:event) into wmbs', self.wmSpec.name(),
                                                                                            self.mask['FirstRun'],
                                                                                            self.mask['FirstLumi'],
                                                                                            self.mask['FirstEvent'],
                                                                                            self.mask['LastRun'],
                                                                                            self.mask['LastLumi'],
                                                                                            self.mask['LastEvent'])
            addedFiles = self.addMCFakeFile()

        self.commitTransaction(existingTransaction=False)

        return sub, addedFiles

    def addFiles(self, block):
        """
        _addFiles_

        create wmbs files from given dbs block.
        as well as run lumi update
        """

        if self.topLevelTask.getInputACDC():
            self.isDBS = False
            for acdcFile in self.validFiles(block['Files']):
                self._addACDCFileToWMBSFile(acdcFile)
        else:
            self.isDBS = True
            for dbsFile in self.validFiles(block['Files']):
                self._addDBSFileToWMBSFile(dbsFile, block['PhEDExNodeNames'])

        # Add files to WMBS
        totalFiles = self.topLevelFileset.addFilesToWMBSInBulk(self.wmbsFilesToCreate,
                                                               self.wmSpec.name(),
                                                               isDBS=self.isDBS)
        # Add files to DBSBuffer
        self._createFilesInDBSBuffer()

        self.topLevelFileset.markOpen(block.get('IsOpen', False))
        return totalFiles

    def getMergeOutputMapping(self):
        """
        _getMergeOutputMapping_

        retrieves the relationship between primary
        dataset and merge output fileset ids for
        all merge tasks created
        """
        return self.mergeOutputMapping

    def _createWorkflowsInDBSBuffer(self):
        """
        _createWorkflowsInDBSBuffer_

        Register workflow information and settings in dbsbuffer for all
        tasks that will potentially produce any output in this spec.
        """

        for task in self.wmSpec.listOutputProducingTasks():
            workflow_id = self.dbsInsertWorkflow.execute(self.wmSpec.name(), task,
                                                         self.wmSpec.getBlockCloseMaxWaitTime(),
                                                         self.wmSpec.getBlockCloseMaxFiles(),
                                                         self.wmSpec.getBlockCloseMaxEvents(),
                                                         self.wmSpec.getBlockCloseMaxSize(),
                                                         conn=self.getDBConn(), transaction=self.existingTransaction())
            if task == self.topLevelTask.getPathName():
                self.topLevelTaskDBSBufferId = workflow_id

    def _createDatasetSubscriptionsInDBSBuffer(self):
        """
        _createDatasetSubscriptionsInDBSBuffer_

        Insert the subscriptions defined in the workload for the output
        datasets with the different options.
        """
        subInfo = self.wmSpec.getSubscriptionInformation()
        for dataset in subInfo:
            dbsDataset = DBSBufferDataset(path=dataset)
            dbsDataset.create()
            dbsDataset.addSubscription(subInfo[dataset])
        return

    def _createFilesInDBSBuffer(self):
        """
        _createFilesInDBSBuffer_

        It does the actual job of creating things in DBSBuffer

        """
        if len(self.dbsFilesToCreate) == 0:
            # Whoops, nothing to do!
            return

        dbsFileTuples = []
        dbsFileLoc = []
        dbsCksumBinds = []
        locationsToAdd = []
        selfChecksums = None

        # The first thing we need to do is add the datasetAlgo
        # Assume all files in a pass come from one datasetAlgo?
        if self.insertedBogusDataset == -1:
            self.insertedBogusDataset = self.dbsFilesToCreate[0].insertDatasetAlgo()

        for dbsFile in self.dbsFilesToCreate:
            # Append a tuple in the format specified by DBSBufferFiles.Add
            # Also run insertDatasetAlgo

            lfn = dbsFile['lfn']
            selfChecksums = dbsFile['checksums']

            newTuple = (lfn, dbsFile['size'],
                        dbsFile['events'], self.insertedBogusDataset,
                        dbsFile['status'], self.topLevelTaskDBSBufferId)

            if newTuple not in dbsFileTuples:
                dbsFileTuples.append(newTuple)

            if len(dbsFile['newlocations']) < 1:
                msg = ''
                msg += "File created without any locations!\n"
                msg += "File lfn: %s\n" % (lfn)
                msg += "Rejecting this group of files in DBS!\n"
                logging.error(msg)
                raise WorkQueueWMBSException(msg)

            for jobLocation in dbsFile['newlocations']:
                if jobLocation not in self.addedLocations:
                    # If we don't have it, try and add it
                    locationsToAdd.append(jobLocation)
                    self.addedLocations.append(jobLocation)
                dbsFileLoc.append({'lfn': lfn, 'pnn': jobLocation})

            if selfChecksums:
                # If we have checksums we have to create a bind
                # For each different checksum
                for entry in selfChecksums.keys():
                    dbsCksumBinds.append({'lfn': lfn, 'cksum': selfChecksums[entry],
                                          'cktype': entry})

        for jobLocation in locationsToAdd:
            self.dbsInsertLocation.execute(siteName=jobLocation,
                                           conn=self.getDBConn(),
                                           transaction=self.existingTransaction())

        self.dbsCreateFiles.execute(files=dbsFileTuples,
                                    conn=self.getDBConn(),
                                    transaction=self.existingTransaction())

        self.dbsSetLocation.execute(binds=dbsFileLoc,
                                    conn=self.getDBConn(),
                                    transaction=self.existingTransaction())

        if len(dbsCksumBinds) > 0:
            self.dbsSetChecksum.execute(bulkList=dbsCksumBinds,
                                        conn=self.getDBConn(),
                                        transaction=self.existingTransaction())

        # Now that we've created those files, clear the list
        self.dbsFilesToCreate = []
        return

    def _addToDBSBuffer(self, dbsFile, checksums, locations):
        """
        This step is just for increase the performance for
        Accountant doesn't neccessary to check the parentage
        """
        dbsBuffer = DBSBufferFile(lfn=dbsFile["LogicalFileName"],
                                  size=dbsFile["FileSize"],
                                  events=dbsFile["NumberOfEvents"],
                                  checksums=checksums,
                                  locations=locations,
                                  status="GLOBAL")
        dbsBuffer.setDatasetPath('bogus')
        dbsBuffer.setAlgorithm(appName="cmsRun", appVer="Unknown",
                               appFam="Unknown", psetHash="Unknown",
                               configContent="Unknown")

        if not dbsBuffer.exists():
            self.dbsFilesToCreate.append(dbsBuffer)
        # dbsBuffer.create()
        return

    def _addDBSFileToWMBSFile(self, dbsFile, storageElements, inFileset=True):
        """
        There are two assumptions made to make this method behave properly,
        1. DBS returns only one level of ParentList.
           If DBS returns multiple level of parentage, it will be still get handled.
           However that might not be what we wanted. In that case, restrict to one level.
        2. Assumes parents files are in the same location as child files.
           This is not True in general case, but workquue should only select work only
           where child and parent files are in the same location
        """
        wmbsParents = []
        dbsFile.setdefault("ParentList", [])
        for parent in dbsFile["ParentList"]:
            wmbsParents.append(self._addDBSFileToWMBSFile(parent,
                                                          storageElements, inFileset=False))

        checksums = {}
        if dbsFile.get('Checksum'):
            checksums['cksum'] = dbsFile['Checksum']
        if dbsFile.get('Adler32'):
            checksums['adler32'] = dbsFile['Adler32']

        wmbsFile = File(lfn=dbsFile["LogicalFileName"],
                        size=dbsFile["FileSize"],
                        events=dbsFile["NumberOfEvents"],
                        checksums=checksums,
                        # TODO: need to get list of parent lfn
                        parents=wmbsParents,
                        locations=set(storageElements))

        for lumi in dbsFile['LumiList']:
            if isinstance(lumi['LumiSectionNumber'], list):
                run = Run(lumi['RunNumber'], *lumi['LumiSectionNumber'])
            else:
                run = Run(lumi['RunNumber'], lumi['LumiSectionNumber'])
            wmbsFile.addRun(run)

        self._addToDBSBuffer(dbsFile, checksums, storageElements)

        logging.info("WMBS File: %s\n on Location: %s", wmbsFile['lfn'], wmbsFile['newlocations'])

        wmbsFile['inFileset'] = bool(inFileset)
        self.wmbsFilesToCreate.append(wmbsFile)

        return wmbsFile

    def _convertACDCFileToDBSFile(self, acdcFile):
        """
        convert ACDCFiles to dbs file format
        """
        dbsFile = {}
        dbsFile["LogicalFileName"] = acdcFile["lfn"]
        dbsFile["FileSize"] = acdcFile["size"]
        dbsFile["NumberOfEvents"] = acdcFile["events"]
        return dbsFile

    def _addACDCFileToWMBSFile(self, acdcFile, inFileset=True):
        """
        adds the ACDC files into WMBS database
        """
        wmbsParents = []
        for parent in acdcFile["parents"]:
            parent = self._addACDCFileToWMBSFile(DatastructFile(lfn=parent,
                                                                locations=acdcFile["locations"]),
                                                 inFileset=False)
            wmbsParents.append(parent)

        # pass empty check sum since it won't be updated to dbs anyway
        checksums = {}
        wmbsFile = File(lfn=str(acdcFile["lfn"]),
                        size=acdcFile["size"],
                        events=acdcFile["events"],
                        first_event=acdcFile.get('first_event', 0),
                        last_event=acdcFile.get('last_event', 0),
                        checksums=checksums,
                        parents=wmbsParents,
                        locations=acdcFile["locations"],
                        merged=acdcFile.get('merged', True))

        ## TODO need to get the lumi lists
        for run in acdcFile['runs']:
            wmbsFile.addRun(run)

        dbsFile = self._convertACDCFileToDBSFile(acdcFile)
        self._addToDBSBuffer(dbsFile, checksums, acdcFile["locations"])

        logging.info("WMBS File: %s\n on Location: %s", wmbsFile['lfn'], wmbsFile['newlocations'])

        wmbsFile['inFileset'] = bool(inFileset)

        self.wmbsFilesToCreate.append(wmbsFile)

        return wmbsFile

    def validFiles(self, files):
        """
        Apply lumi mask and or run white/black list and return files which have
        one or more of the requested lumis
        """
        runWhiteList = self.topLevelTask.inputRunWhitelist()
        runBlackList = self.topLevelTask.inputRunBlacklist()
        lumiMask = self.topLevelTask.getLumiMask()

        blackMask = None
        if lumiMask:  # We have a lumiMask, so use it and modify with run white/black list
            if runWhiteList:
                lumiMask.selectRuns(runWhiteList)
            if runBlackList:
                lumiMask.removeRuns(runBlackList)
        elif runWhiteList:  # We have a run whitelist, subtract off blacklist
            lumiMask = LumiList(runs=runWhiteList)
            if runBlackList:  # We only have a blacklist, so make a black mask out of it instead
                lumiMask.removeRuns(runBlackList)
        else:
            lumiMask = None
            if runBlackList:
                blackMask = LumiList(runs=runBlackList)

        results = []
        for f in files:
            if isinstance(f, basestring) or "LumiList" not in f:
                results.append(f)
                continue

            # Create a LumiList from the WMBS info
            runLumis = {}
            for x in f['LumiList']:
                if x['RunNumber'] in runLumis:
                    runLumis[x['RunNumber']].extend(x['LumiSectionNumber'])
                else:
                    runLumis[x['RunNumber']] = x['LumiSectionNumber']
            fileLumiList = LumiList(runsAndLumis=runLumis)

            if lumiMask:
                if fileLumiList & lumiMask:  # At least one lumi from file is in lumiMask
                    results.append(f)
            elif blackMask:
                if fileLumiList - blackMask:  # At least one lumi from file is not in blackMask
                    results.append(f)
            else:  # There is effectively no mask
                results.append(f)

        return results
