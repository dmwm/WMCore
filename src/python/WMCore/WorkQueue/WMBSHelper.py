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

from WMCore.Agent.Configuration import Configuration
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.HTTPFrontEnd.WMBS.External.CouchDBSource.CouchDBConnectionBase \
     import CouchDBConnectionBase

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

def killWorkflow(workflowName):
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

    config = Configuration()
    config.section_("JobStateMachine")
    config.JobStateMachine.couchurl = CouchDBConnectionBase.getCouchDBURL()
    config.JobStateMachine.couchDBName = CouchDBConnectionBase.getCouchDBName()
    changeState = ChangeState(config)

    for liveJob in liveJobs:
        liveWMBSJob = Job(id = liveJob["id"])
        changeState.propagate(liveWMBSJob, "killed", liveJob["state"])

    if not existingTransaction:
        myThread.transaction.commit()
    return

class WMBSHelper(WMConnectionBase):
    """
    DAO equipped class that interfaces between the WorkQueue (and DBS),
    and WMBS

    """

    def __init__(self, wmSpec, wmSpecUrl, wmSpecOwner, taskName, 
                 taskType, whitelist, blacklist, blockName, mask):
        #TODO: 
        # 1. get the top level task.
        # 2. get the top level step and input
        # 3. generated the spec, owner, name from task
        # 4. get input file list from top level step
        # 5. generate the file set from work flow.
        self.wmSpecName = wmSpec.name()
        self.wmSpecUrl = wmSpecUrl
        self.wmSpecOwner = wmSpecOwner
        self.topLevelTaskName = taskName
        self.topLevelTaskType = taskType
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.block = blockName or None
        self.mask = mask or None
        self.topLevelFileset = None
        self.topLevelSubscription = None    
        self.topLevelTask = wmSpec.getTask(self.topLevelTaskName)


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
        self.addToFileset            = self.daofactory(classname = "Files.AddToFileset")
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

    def createSubscription(self, topLevelFilesetName = None):
        self.createTopLevelFileset(topLevelFilesetName)
        return self._createChildSubscription(self.topLevelTask, self.topLevelFileset)
        
    def _createChildSubscription(self, task, fileset):
        # create workflow
        # make up workflow name from wmspec name
        workflow = Workflow(self.wmSpecUrl, self.wmSpecOwner, 
                                 self.wmSpecName,
                                 task.getPathName())
        workflow.create()
        subs = Subscription(fileset = fileset, workflow = workflow,
                            split_algo = task.jobSplittingAlgorithm(),
                            type = task.taskType())
        subs.create()
        
        if self.topLevelSubscription == None:
            self.topLevelSubscription = subs
            logging.info("Top level subscription created: %s" % subs['id'])
        else:
            logging.info("Child subscription created: %s" % subs['id'])
        
        # To do: check this is the right change
        #outputModules =  task.getOutputModulesForStep(task.getTopStepName())
        outputModules = task.getOutputModulesForTask()
        for outputModule in outputModules:
            for outputModuleName in outputModule.listSections_():
                if task.taskType() == "Merge":
                    outputFilesetName = "%s/merged-%s" % (task.getPathName(),
                                                          outputModuleName)
                else:
                    outputFilesetName = "%s/unmerged-%s" % (task.getPathName(),
                                                            outputModuleName)
        
                outputFileset = Fileset(name = outputFilesetName)
                outputFileset.create()
                # this will reopen child fileset every time 
                # the fileset exist already 
                outputFileset.markOpen(True)
                workflow.addOutput(outputModuleName, outputFileset)
        
                for childTask in task.childTaskIterator():
                    if childTask.data.input.outputModule == outputModuleName:
                        self._createChildSubscription(childTask, outputFileset) 
            
        return self.topLevelSubscription

    def createTopLevelFileset(self, topLevelFilesetName = None):
        """
        _createTopLevelFileset_

        Create the top level fileset for the workflow.  If the name of the top
        level fileset is not given create one.
        """
        if topLevelFilesetName == None:
            filesetName = ("%s-%s" % (self.wmSpecName, self.topLevelTaskName))
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

    def addMCFakeFile(self):
        """Add a fake file for wmbs to run production over"""
        needed = ['FirstEvent', 'FirstLumi', 'FirstRun', 'LastEvent', 'LastLumi', 'LastRun']
        for key in needed:
            if self.mask and self.mask.get(key) is None:
                raise RuntimeError, 'Invalid value "%s" for %s' % (self.mask.get(key), key)
        if not self.topLevelTask.siteWhitelist():
            raise RuntimeError, "Site whitelist mandatory for MonteCarlo"
        locations = set()
        for site in self.topLevelTask.siteWhitelist():
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

    def createSubscriptionAndAddFiles(self, dbsBlock):
        """
        _createSubscriptionAndAddFiles_
        
        Create the subscription and add files at one time to
        put everything in one transaction.

        """

        self.beginTransaction()
        
        sub = self.createSubscription()
        
        if dbsBlock != None:
            self.addFiles(dbsBlock)
        #For MC case
        else:
            self.addMCFakeFile()
        
        self.commitTransaction(existingTransaction = False)

        return sub
    
    def addFiles(self, dbsBlock):
        """
        _createFiles_
        
        create wmbs files from given dbs block.
        as well as run lumi update
        """

        for dbsFile in self.validFiles(dbsBlock['Files']):
            self._convertDBSFileToWMBSFile(dbsFile, dbsBlock['StorageElements'])


        # Add files to WMBS
        totalFiles = self._addFilesToWMBSInBulk()
        # Add files to DBSBuffer
        self._createFilesInDBSBuffer()

        #self.topLevelFileset.commit()
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

        
        for wmbsFile in self.wmbsFilesToCreate:
            lfn           = wmbsFile['lfn']

            if wmbsFile['inFileset']:
                fileLFNs.append(lfn)
                for parent in wmbsFile['parents']:
                    parentageBinds.append({'child': lfn, 'parent': parent['lfn']})
            
            if wmbsFile.exists():
                continue


            

            selfChecksums = wmbsFile['checksums']
            #parentageBinds.append({'child': lfn, 'jobid': wmbsFile['id']})
            runLumiBinds.append({'lfn': lfn, 'runs': wmbsFile['runs']})


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
            
            
            
            self.setFileRunLumi.execute(file = runLumiBinds,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
            
            self.setFileAddChecksum.execute(bulkList = fileCksumBinds,
                                            conn = self.getDBConn(),
                                            transaction = self.existingTransaction())
            
            
            self.setFileLocation.execute(lfn = fileLocations,
                                         conn = self.getDBConn(),
                                         transaction = self.existingTransaction())


            

        if len(fileLFNs) > 0:
            logging.debug("About to add %i files to fileset %i" % (len(fileLFNs),
                                                                   self.topLevelFileset.id))
            self.addToFileset.execute(file = fileLFNs,
                                      fileset = self.topLevelFileset.id,
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
            
            dbsFileTuples.append((lfn, dbsFile['size'],
                                  dbsFile['events'], self.insertedBogusDataset,
                                  dbsFile['status']))


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
                                  status = "AlreadyInDBS")
        dbsBuffer.setDatasetPath('bogus')
        dbsBuffer.setAlgorithm(appName = "cmsRun", appVer = "Unknown", 
                             appFam = "Unknown", psetHash = "Unknown", 
                             configContent = "Unknown")

        if not dbsBuffer.exists():        
            self.dbsFilesToCreate.append(dbsBuffer)
        #dbsBuffer.create()
        return
    
    def _convertDBSFileToWMBSFile(self, dbsFile, storageElements, inFileset = True):
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
            wmbsParents.append(self._convertDBSFileToWMBSFile(parent, storageElements, inFileset = False))
        
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
        

    def validFiles(self, files):
        """Apply run white/black list and return valid files"""
        runWhiteList = self.topLevelTask.inputRunWhitelist()
        runBlackList = self.topLevelTask.inputRunBlacklist()
        results = []
        for f in files:
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
