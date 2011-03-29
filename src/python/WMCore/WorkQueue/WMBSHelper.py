#!/usr/bin/env python
#pylint: disable-msg=W6501, E1103, C0103
# E1103: Attach methods to threads
# W6501: Allow logging messages to have string formatting
# C0103: Internal method names start with '_'
"""
_WMBSHelper_

Use WMSpecParser to extract information for creating workflow, fileset, and subscription
"""

import logging
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Job import Job
from WMCore.WMException import WMException
from WMCore.Services.UUID import makeUUID
from WMCore.DataStructs.Run import Run

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile
# Added to allow bulk commits
from WMCore.DAOFactory           import DAOFactory
from WMCore.WMConnectionBase     import WMConnectionBase
from WMCore.JobStateMachine.ChangeState import ChangeState

from WMCore.BossAir.BossAirAPI    import BossAirAPI, BossAirException


def wmbsSubscriptionStatus(logger, dbi, conn, transaction):
    """Function to return status of wmbs subscriptions
    """
    action = DAOFactory(package = 'WMBS',
                        logger = logger,
                        dbinterface = dbi)('Monitoring.SubscriptionStatus')
    return action.execute(conn = conn,
                          transaction = transaction)



class WorkQueueWMBSException(WMException):
    """
    Dummy exception class for exceptions raised
    in WMBS Helper.

    TODO: Do something useful

    """

    pass

def killWorkflow(workflowName, jobCouchConfig, bossAirConfig = None):
    """
    _killWorkflow_

    Kill a workflow that is already executing inside the agent.  This will
    mark all incomplete jobs as failed and files that belong to all
    non-cleanup and non-logcollect subscriptions as failed.  The name of the
    JSM couch database and the URL to the database must be passed in as well
    so the state transitions are logged.
    """
    myThread = threading.currentThread()    
    daoFactory = DAOFactory(package = "WMCore.WMBS",
                            logger = myThread.logger,
                            dbinterface = myThread.dbi)    
    killFilesAction = daoFactory(classname = "Subscriptions.KillWorkflow")
    killJobsAction = daoFactory(classname = "Jobs.KillWorkflow")

    existingTransaction = False
    if myThread.transaction.conn:
        existingTransaction = True
    else:
        myThread.transaction.begin()

    killFilesAction.execute(workflowName = workflowName,
                            conn = myThread.transaction.conn,
                            transaction = True)

    liveJobs = killJobsAction.execute(workflowName = workflowName,
                                      conn = myThread.transaction.conn,
                                      transaction = True)

    changeState = ChangeState(jobCouchConfig)

    # Deal with any jobs that are running in the batch system
    # only works if we can start the API
    if bossAirConfig:
        bossAir = BossAirAPI(config = bossAirConfig, noSetup = True)
        killableJobs = []
        for liveJob in liveJobs:
            if liveJob["state"].lower() == 'executing':
                # Then we need to kill this on the batch system
                liveWMBSJob = Job(id = liveJob["id"])
                changeState.propagate(liveWMBSJob, "killed", liveJob["state"])
                killableJobs.append(liveJob)
        # Now kill them
        try:
            bossAir.kill(jobs = killableJobs)
        except BossAirException, ex:
            # Something's gone wrong
            # Jobs not killed!
            logging.error("Error while trying to kill running jobs in workflow!\n")
            logging.error(str(ex))
            trace = getattr(ex, 'traceback', '')
            logging.error(trace)
            # But continue; we need to kill the jobs in the master
            # the batch system will have to take care of itself.
            pass

    for liveJob in liveJobs:
        if liveJob["state"] == "killed":
            # Then we've killed it already
            continue
        liveWMBSJob = Job(id = liveJob["id"])
        changeState.propagate(liveWMBSJob, "killed", liveJob["state"])

    if not existingTransaction:
        myThread.transaction.commit()
    return

def freeSlots(multiplier = 1.0):
    """
    Get free resources from wmbs.

    Specify multiplier to apply a ratio to the actual numbers
    """
    from WMCore.ResourceControl.ResourceControl import ResourceControl
    rc_sites = ResourceControl().listThresholdsForCreate()
    sites = {}
    [sites.__setitem__(name, multiplier * slots['total_slots'])
            for name, slots in rc_sites.items() if slots['total_slots'] > 0]
    return sites

class WMBSHelper(WMConnectionBase):
    """
    _WMBSHelper_

    Interface between the WorkQueue and WMBS.
    """
    def __init__(self, wmSpec, blockName = None, mask = None):
        """
        _init_

        Initialize DAOs and other things needed.
        """
        self.block = blockName
        self.mask = mask
        self.wmSpec = wmSpec

        self.topLevelFileset = None
        self.topLevelSubscription = None
        
        # Initiate the pieces you need to run your own DAOs
        WMConnectionBase.__init__(self, "WMCore.WMBS")
        myThread = threading.currentThread()
        self.dbsDaoFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)
        self.uploadFactory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)


        # DAOs from WMBS for file commit
        self.setParentage            = self.daofactory(classname = "Files.SetParentage")
        self.setFileRunLumi          = self.daofactory(classname = "Files.AddRunLumi")
        self.setFileLocation         = self.daofactory(classname = "Files.SetLocationByLFN")
        self.setFileAddChecksum      = self.daofactory(classname = "Files.AddChecksumByLFN")
        self.addFileAction           = self.daofactory(classname = "Files.Add")
        self.addToFileset            = self.daofactory(classname = "Files.AddDupsToFileset")
        self.getLocationInfo         = self.daofactory(classname = "Locations.GetSiteInfo")

        # DAOs from DBSBuffer for file commit
        self.dbsCreateFiles    = self.dbsDaoFactory(classname = "DBSBufferFiles.Add")
        self.dbsSetLocation    = self.dbsDaoFactory(classname = "DBSBufferFiles.SetLocationByLFN")
        self.dbsInsertLocation = self.dbsDaoFactory(classname = "DBSBufferFiles.AddLocation")
        self.dbsSetChecksum    = self.dbsDaoFactory(classname = "DBSBufferFiles.AddChecksumByLFN")


        # Added for file creation bookkeeping
        self.dbsFilesToCreate     = []
        self.addedLocations       = []
        self.wmbsFilesToCreate    = []
        self.insertedBogusDataset = -1

        return

    def createTopLevelFileset(self, topLevelFilesetName = None):
        """
        _createTopLevelFileset_

        Create the top level fileset for the workflow.  If the name of the top
        level fileset is not given create one.
        """
        if topLevelFilesetName == None:
            filesetName = ("%s-%s" % (self.wmSpec.name(),
                                      self.wmSpec.getTopLevelTask()[0].name()))
            if self.block:
                filesetName += "-%s" % self.block
            else:
                #create empty fileset for production job
                filesetName += "-%s" % makeUUID()
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

    def createSubscription(self, topLevelFilesetName = None, task = None,
                           fileset = None):
        """
        _createSubscription_

        Create subscriptions in WMBS for all the tasks in the spec.  This
        includes filesets, workflows and the output map for each task.
        """
        if task == None or fileset == None:
            self.createTopLevelFileset(topLevelFilesetName)
            sub = None
            for topLevelTask in self.wmSpec.getTopLevelTask():
                sub = self.createSubscription(topLevelFilesetName,
                                              topLevelTask,
                                              self.topLevelFileset)
            return sub

        workflow = Workflow(self.wmSpec.specUrl(), self.wmSpec.getOwner()["name"],
                            self.wmSpec.getOwner().get("dn", None), self.wmSpec.name(), task.getPathName())
        workflow.create()
        subscription = Subscription(fileset = fileset, workflow = workflow,
                                    split_algo = task.jobSplittingAlgorithm(),
                                    type = task.taskType())
        subscription.create()
        for site in task.siteWhitelist():
            subscription.addWhiteBlackList([{"site_name": site, "valid": True}])

        for site in task.siteBlacklist():
            subscription.addWhiteBlackList([{"site_name": site, "valid": False}])            
        
        if self.topLevelSubscription == None:
            self.topLevelSubscription = subscription
            logging.info("Top level subscription created: %s" % subscription["id"])
        else:
            logging.info("Child subscription created: %s" % subscription["id"])
        
        outputModules = task.getOutputModulesForTask()
        for outputModule in outputModules:
            for outputModuleName in outputModule.listSections_():
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
                                                         
                        self.createSubscription(topLevelFilesetName, childTask, outputFileset) 

                if mergedOutputFileset == None:
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
                raise RuntimeError, 'Invalid value "%s" for %s' % (self.mask.get(key), key)
        if not self.wmSpec.getTopLevelTask()[0].siteWhitelist():
            raise RuntimeError, "Site whitelist mandatory for MonteCarlo"
        locations = set()
        for site in self.wmSpec.getTopLevelTask()[0].siteWhitelist():
            try:
                siteInfo = self.getLocationInfo.execute(site, conn = self.getDBConn(),
                                       transaction = self.existingTransaction())
                if not siteInfo:
                    self.logger.info('Skipping MonteCarlo injection to site "%s" as unknown to wmbs' % site)
                    continue
                locations.add(siteInfo[0]['se_name'])
            except StandardError, ex:
                self.logger.error('Error getting storage element for "%s": %s' % (site, str(ex)))
        if not locations:
            raise RuntimeError, "No locations to inject Monte Carlo work to, unable to proceed"
        mcFakeFileName = "MCFakeFile-%s" % makeUUID()
        wmbsFile = File(lfn = mcFakeFileName,
                        first_event = self.mask['FirstEvent'],
                        last_event = self.mask['LastEvent'],
                        events = self.mask['LastEvent'] - self.mask['FirstEvent'] + 1, # inclusive range
                        locations = locations,
                        merged = False, # merged causes dbs parentage relation
                        )

        if self.mask:
            lumis = range(self.mask['FirstLumi'], self.mask['LastLumi'] + 1) # inclusive range
            wmbsFile.addRun(Run(self.mask['FirstRun'], *lumis)) # assume run number static
        else:
            wmbsFile.addRun(Run(1, 1))

        wmbsFile['inFileset'] = True # file is not a parent

        self.wmbsFilesToCreate.append(wmbsFile)

        totalFiles = self._addFilesToWMBSInBulk()

        self.topLevelFileset.markOpen(False)
        return totalFiles

    def createSubscriptionAndAddFiles(self, block):
        """
        _createSubscriptionAndAddFiles_
        
        Create the subscription and add files at one time to
        put everything in one transaction.

        """
        self.beginTransaction()
        
        sub = self.createSubscription()
        
        if block != None:
            self.addFiles(block)
        #For MC case
        else:
            self.addMCFakeFile()
        
        self.commitTransaction(existingTransaction = False)

        return sub
    
    def addFiles(self, block, workflow = None):
        """
        _addFiles_
        
        create wmbs files from given dbs block.
        as well as run lumi update
        """
                
        if self.wmSpec.getTopLevelTask()[0].getInputACDC():
            for acdcFile in self.validFiles(block):
                self._addACDCFileToWMBSFile(acdcFile)
        else:
            for dbsFile in self.validFiles(block['Files']):
                self._addDBSFileToWMBSFile(dbsFile, block['StorageElements'])


        # Add files to WMBS
        totalFiles = self._addFilesToWMBSInBulk()
        # Add files to DBSBuffer
        self._createFilesInDBSBuffer()

        self.topLevelFileset.markOpen(False)
        return totalFiles


    def _addFilesToWMBSInBulk(self):
        """
        _addFilesToWMBSInBulk

        Do a bulk addition of files into WMBS
        """

        if len(self.wmbsFilesToCreate) == 0:
            # Nothing to do
            return 0


        parentageBinds = []
        runLumiBinds   = []
        fileCksumBinds = []
        fileLocations  = []
        fileCreate     = []
        fileLFNs       = []
        lfnsToCreate   = []

        
        for wmbsFile in self.wmbsFilesToCreate:
            lfn           = wmbsFile['lfn']

            if wmbsFile['inFileset']:
                if not lfn in fileLFNs:
                    fileLFNs.append(lfn)
                    for parent in wmbsFile['parents']:
                        parentageBinds.append({'child': lfn, 'parent': parent['lfn']})
            
            selfChecksums = wmbsFile['checksums']
            if len(wmbsFile['runs']) > 0:
                runLumiBinds.append({'lfn': lfn, 'runs': wmbsFile['runs']})

            if wmbsFile.exists():
                continue

            if lfn in lfnsToCreate:
                continue
            lfnsToCreate.append(lfn)

            if len(wmbsFile['newlocations']) < 1:
                # Then we're in trouble
                msg = "File created in WMBS without locations!\n"
                msg += "File lfn: %s\n" % (lfn)
                logging.error(msg)
                raise WorkQueueWMBSException(msg)

            for loc in wmbsFile['newlocations']:
                fileLocations.append({'lfn': lfn, 'location': loc})

            if selfChecksums:
                # If we have checksums we have to create a bind
                # For each different checksum
                for entry in selfChecksums.keys():
                    fileCksumBinds.append({'lfn': lfn, 'cksum' : selfChecksums[entry],
                                           'cktype' : entry})

            fileCreate.append([lfn,
                               wmbsFile['size'],
                               wmbsFile['events'],
                               None,
                               wmbsFile["first_event"],
                               wmbsFile["last_event"],
                               wmbsFile['merged']])

        if len(fileCreate) > 0:
            self.addFileAction.execute(files = fileCreate,
                                       conn = self.getDBConn(),
                                       transaction = self.existingTransaction())
            self.setFileAddChecksum.execute(bulkList = fileCksumBinds,
                                            conn = self.getDBConn(),
                                            transaction = self.existingTransaction())
            self.setFileLocation.execute(lfn = fileLocations,
                                         conn = self.getDBConn(),
                                         transaction = self.existingTransaction())

        if len(runLumiBinds) > 0:
            self.setFileRunLumi.execute(file = runLumiBinds,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
            

        if len(fileLFNs) > 0:
            logging.debug("About to add %i files to fileset %i" % (len(fileLFNs),
                                                                   self.topLevelFileset.id))
            self.addToFileset.execute(file = fileLFNs,
                                      fileset = self.topLevelFileset.id,
                                      workflow = self.wmSpec.name(),
                                      conn = self.getDBConn(),
                                      transaction = self.existingTransaction())

        if len(parentageBinds) > 0:
            self.setParentage.execute(binds = parentageBinds,
                                      conn = self.getDBConn(),
                                      transaction = self.existingTransaction())


        

        return len(fileCreate)


    def _createFilesInDBSBuffer(self):
        """
        _createFilesInDBSBuffer_
        
        It does the actual job of creating things in DBSBuffer
        
        """
        if len(self.dbsFilesToCreate) == 0:
            # Whoops, nothing to do!
            return

        dbsFileTuples  = []
        dbsFileLoc     = []
        dbsCksumBinds  = []
        locationsToAdd = []
        selfChecksums  = None


        # The first thing we need to do is add the datasetAlgo
        # Assume all files in a pass come from one datasetAlgo?
        if self.insertedBogusDataset  == -1:
            self.insertedBogusDataset = self.dbsFilesToCreate[0].insertDatasetAlgo()



        for dbsFile in self.dbsFilesToCreate:
            # Append a tuple in the format specified by DBSBufferFiles.Add
            # Also run insertDatasetAlgo

            lfn           = dbsFile['lfn']
            selfChecksums = dbsFile['checksums']

            newTuple = (lfn, dbsFile['size'],
                        dbsFile['events'], self.insertedBogusDataset,
                        dbsFile['status'])

            if not newTuple in dbsFileTuples:
                dbsFileTuples.append(newTuple)


            if len(dbsFile['newlocations']) < 1:
                msg = ''
                msg += "File created without any locations!\n"
                msg += "File lfn: %s\n" % (lfn)
                msg += "Rejecting this group of files in DBS!\n"
                logging.error(msg)
                raise WorkQueueWMBSException(msg)
                

            for jobLocation in dbsFile['newlocations']:
                if not jobLocation in self.addedLocations:
                    # If we don't have it, try and add it
                    locationsToAdd.append(jobLocation)
                    self.addedLocations.append(jobLocation)
                dbsFileLoc.append({'lfn': lfn, 'sename' : jobLocation})
            
            if selfChecksums:
                # If we have checksums we have to create a bind
                # For each different checksum
                for entry in selfChecksums.keys():
                    dbsCksumBinds.append({'lfn': lfn, 'cksum' : selfChecksums[entry],
                                          'cktype' : entry})

        for jobLocation in locationsToAdd:
            self.dbsInsertLocation.execute(siteName = jobLocation,
                                           conn = self.getDBConn(),
                                           transaction = self.existingTransaction())

        self.dbsCreateFiles.execute(files = dbsFileTuples,
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())


        self.dbsSetLocation.execute(binds = dbsFileLoc,
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

        if len(dbsCksumBinds) > 0:
            self.dbsSetChecksum.execute(bulkList = dbsCksumBinds,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())


        # Now that we've created those files, clear the list
        self.dbsFilesToCreate = []
        return
        
    def _addToDBSBuffer(self, dbsFile, checksums, locations):
        """
        This step is just for increase the performance for 
        Accountant doesn't neccessary to check the parentage
        """
        dbsBuffer = DBSBufferFile(lfn = dbsFile["LogicalFileName"], 
                                  size = dbsFile["FileSize"],
                                  events = dbsFile["NumberOfEvents"], 
                                  checksums = checksums,
                                  locations = locations, 
                                  status = "GLOBAL")
        dbsBuffer.setDatasetPath('bogus')
        dbsBuffer.setAlgorithm(appName = "cmsRun", appVer = "Unknown", 
                             appFam = "Unknown", psetHash = "Unknown", 
                             configContent = "Unknown")

        if not dbsBuffer.exists():        
            self.dbsFilesToCreate.append(dbsBuffer)
        #dbsBuffer.create()
        return
    
    def _addDBSFileToWMBSFile(self, dbsFile, storageElements, inFileset = True):
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
        
        for parent in dbsFile["ParentList"]:
            wmbsParents.append(self._addDBSFileToWMBSFile(parent, 
                                            storageElements, inFileset = False))
        
        checksums = {}
        if dbsFile.get('Checksum'):
            checksums['cksum'] = dbsFile['Checksum']
        if dbsFile.get('Adler32'):
            checksums['adler32'] = dbsFile['Adler32']
            
        wmbsFile = File(lfn = dbsFile["LogicalFileName"],
                        size = dbsFile["FileSize"],
                        events = dbsFile["NumberOfEvents"],
                        checksums = checksums,
                        #TODO: need to get list of parent lfn
                        parents = wmbsParents,
                        locations = set(storageElements))
        
        for lumi in dbsFile['LumiList']:
            run = Run(lumi['RunNumber'], lumi['LumiSectionNumber']) 
            wmbsFile.addRun(run)
        
        self._addToDBSBuffer(dbsFile, checksums, storageElements)
            
        logging.info("WMBS File: %s\n on Location: %s" 
                     % (wmbsFile['lfn'], wmbsFile['newlocations']))

        if inFileset:
            wmbsFile['inFileset'] = True
        else:
            wmbsFile['inFileset'] = False
            
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
        
    def _addACDCFileToWMBSFile(self, acdcFile, inFileset = True):
        """
        """
        wmbsParents = []
        #pass empty check sum since it won't be updated to dbs anyway
        checksums = {}
        wmbsFile = File(lfn = str(acdcFile["lfn"]),
                        size = acdcFile["size"],
                        events = acdcFile["events"],
                        checksums = checksums,
                        #TODO: need to get list of parent lfn
                        parents = acdcFile["parents"],
                        locations = acdcFile["locations"])

        ## TODO need to get the lumi lists
        for run in acdcFile['runs']:
            wmbsFile.addRun(run)
        

        dbsFile = self._convertACDCFileToDBSFile(acdcFile)
        self._addToDBSBuffer(dbsFile, checksums, acdcFile["locations"])
            
        logging.info("WMBS File: %s\n on Location: %s" 
                     % (wmbsFile['lfn'], wmbsFile['locations']))

        if inFileset:
            wmbsFile['inFileset'] = True
        else:
            wmbsFile['inFileset'] = False
            
        self.wmbsFilesToCreate.append(wmbsFile)
        
        return wmbsFile


    def validFiles(self, files):
        """Apply run white/black list and return valid files"""
        runWhiteList = self.wmSpec.getTopLevelTask()[0].inputRunWhitelist()
        runBlackList = self.wmSpec.getTopLevelTask()[0].inputRunBlacklist()

        results = []
        for f in files:
            if type(f) == type("") or not f.has_key("LumiList"):
                results.append(f)
                continue
            if runWhiteList or runBlackList:
                runs = set([x['RunNumber'] for x in f['LumiList']])
                # apply blacklist
                runs = runs.difference(runBlackList)
                # if whitelist only accept listed runs
                if runWhiteList:
                    runs = runs.intersection(runWhiteList)
                # any runs left are ones we will run on, if none ignore file
                if not runs:
                    continue
            results.append(f)
        return results
