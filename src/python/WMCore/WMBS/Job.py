#!/usr/bin/env python
#Turn off too many arguments
#pylint: disable-msg=R0913
"""
_Job_

A job is owned by a jobgroup (which gives it it's workflow) and is 
associated to a (set of) file(s). The job interacts with its jobgroup
to acquire/complete/fail files. A job know's it's Workflow via it's 
jobgroup. A job is meaningless without a jobgroup.

A WMBS job != a job in a batch system, it's more abstract - it's the piece of 
work that needs to get done.

CREATE TABLE wmbs_job (
             id          INTEGER   PRIMARY KEY AUTOINCREMENT,
             jobgroup    INT(11)   NOT NULL,
             name        VARCHAR(255),
             last_update TIMESTAMP NOT NULL,
             FOREIGN KEY (jobgroup) REFERENCES wmbs_jobgroup(id)
               ON DELETE CASCADE)

CREATE TABLE wmbs_job_assoc (
    job    INT(11) NOT NULL,
    file   INT(11) NOT NULL,
    FOREIGN KEY (job) REFERENCES wmbs_job(id)
                    ON DELETE CASCADE,
    FOREIGN KEY (file) REFERENCES wmbs_file(id)
                    ON DELETE CASCADE)

CREATE TABLE wmbs_job_mask (
    job           INT(11)     NOT NULL,
    FirstEvent    INT(11),
    LastEvent     INT(11),
    FirstLumi     INT(11),
    LastLumi      INT(11),
    FirstRun      INT(11),
    LastRun       INT(11),
    inclusivemask BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (job)       REFERENCES wmbs_job(id)
    ON DELETE CASCADE)
    
Jobs are added to the WMBS database by their parent JobGroup, but are 
responsible for updating their state (and name).
"""

__revision__ = "$Id: Job.py,v 1.27 2009/05/08 16:04:10 sfoulkes Exp $"
__version__ = "$Revision: 1.27 $"

import datetime
from sets import Set

from WMCore.DataStructs.Job import Job as WMJob
from WMCore.DataStructs.Fileset import Fileset
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset as WMBSFileset
from WMCore.WMBS.WMBSBase import WMBSBase
from WMCore.Services.UUID import makeUUID

class Job(WMBSBase, WMJob):
    """
    A job in WMBS
    """
    def __init__(self, name = None, files = None, id = -1):
        """
        ___init___
        
        jobgroup object is used to determine the workflow. 
        inputFiles is a list of files that the job will process.
        """
        WMBSBase.__init__(self)
        WMJob.__init__(self, name = name, files = files)

        self.id = id
        return
            
    def create(self, group):
        """
        _create_
        
        Write the job to the database.
        """
        existingTransaction = self.beginTransaction()

        self.job_group = group.id

        if self.name == None:
            self.name = makeUUID()

        jobAction = self.daofactory(classname = "Jobs.New")
        jobAction.execute(self.job_group, self.name,
                          conn = self.getDBConn(),
                          transaction = self.existingTransaction())

        self.load()
        
        maskAction = self.daofactory(classname = "Masks.New")
        maskAction.execute(jobid = self.id, conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        self.save()
        self.commitTransaction(existingTransaction)
        return
        
    def delete(self):
        """
        Remove a job from WMBS
        """
        deleteAction = self.daofactory(classname='Jobs.Delete')
        deleteAction.execute(id = self.id, conn = self.getDBConn(),
                              transaction = self.existingTransaction())
        return

    def save(self):
        """
        _save_

        Flush all changes that have been made to the job to the database.
        """
        existingTransaction = self.beginTransaction()

        saveAction = self.daofactory(classname = "Jobs.Save")
        saveAction.execute(self.id, self.job_group, self.name,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())

        maskAction = self.daofactory(classname = "Masks.Save")
        maskAction.execute(jobid = self.id, mask = self.mask,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())

        self.associateFiles()
        self.commitTransaction(existingTransaction)
        return

    def exists(self):
        """
        _exists_

        Does a job exist with this name.
        """
        if self.id != -1:
            action = self.daofactory(classname='Jobs.ExistsByID')
            result =  action.execute(id = self.id, conn = self.getDBConn(),
                                     transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname='Jobs.Exists')
            result = action.execute(name = self.name, conn = self.getDBConn(),
                                  transaction = self.existingTransaction())
            self.id = result

        return result
                
    def load(self):
        """
        _load_

        Load the job's name, id and jobgroup from the database.  Either the ID
        or the name must be set before this is called.
        """
        if self.id > 0:
            loadAction = self.daofactory(classname = "Jobs.LoadFromID")
            results = loadAction.execute(self.id, conn = self.getDBConn(),
                                         transaction = self.existingTransaction())
        else:
            loadAction = self.daofactory(classname = "Jobs.LoadFromName")
            results = loadAction.execute(self.name, conn = self.getDBConn(),
                                         transaction = self.existingTransaction())

        self.id = results["id"]
        self.name = results["name"]
        self.last_update = results["last_update"]
        self.job_group = results["jobgroup"]

        return

    def loadData(self):
        """
        _loadData_

        Load all information about the job, including the mask and all input
        files.  Either the ID or the name must be specified before this is
        called.
        """
        existingTransaction = self.beginTransaction()
        
        if self.id < 0 or self.name == None:
            self.load()
        
        self.getMask()
        
        fileAction = self.daofactory(classname = "Jobs.LoadFiles")
        files = fileAction.execute(self.id, conn = self.getDBConn(),
                                   transaction = self.existingTransaction())

        self.inputFiles = []
        for file in files:
            newFile = File(id = file["id"])
            newFile.loadData(parentage = 0)
            self.addFile(newFile)

        self.commitTransaction(existingTransaction)
        return
    
    def getMask(self):
        """
        _getMask_
        
        load job mask from database and update the self.mask
        and return the value
        """
        jobMaskAction = self.daofactory(classname = "Masks.Load")
        jobMask = jobMaskAction.execute(self.id, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

        self.mask.update(jobMask)
        return self.mask
    
    def getFiles(self, type = "list"):
        """
        _getFiles_

        Retrieve the files that are associated with this job.  If the id of the
        job is -1 this will skip loading from the database and return whatever
        files currently exist in the object.  If the id has been set this will
        make sure that the files in this object's input files match the files that
        are associated to the job in the database.  If the two do not match then
        this object's fileset will be re-populated with files from the database.
        """
        if self.id < 0:
            return WMJob.getFiles(self, type)
    
        existingTransaction = self.beginTransaction()
        idAction = self.daofactory(classname = "Jobs.LoadFiles")
        fileIDs = idAction.execute(self.id, conn = self.getDBConn(),
                                   transaction = self.existingTransaction())

        currentFileIDs = WMJob.getFiles(self, type = "id") 
        for fileID in fileIDs:
            if fileID["id"] not in currentFileIDs:
                self.loadData()
                break

        self.commitTransaction(existingTransaction)
        return WMJob.getFiles(self, type)

    def processSuccessfulJob(self, file):
        """
        _processSuccessfulJob_
        should be used internally by the job group. 
        update the file parentage
        also add run-lumi information
        take wmbs file object as a argument (doesn't have to be loaded). 
        """
        existingTransaction = self.beginTransaction()

        # doesn't need this process but use as the output file temporary holder
        WMJob.addOutput(self, file)
        
        inputFiles = self.getFiles()
        
        # this doesn't have opitmal performance
        # if the performance is problem access directly DAO object
        # update the parentage and add runRumi
        newRunSet = Set()
        for inputFile in inputFiles:
            inputFile.addChild(file["lfn"])
              
            for inputRun in inputFile.getRuns():
                addFlag = False
                for runMember in newRunSet:
                    if runMember.run == inputRun.run:
                        # this rely on Run object overwrite __add__ to update
                        # self 
                        runMember + inputRun
                        addFlag = True
                        
                if not addFlag:
                    newRunSet.add(inputRun)
        
        file.addRunSet(newRunSet)

        self.commitTransaction(existingTransaction)
        return
           
    def submit(self, name = None):
        """
        _submit_
        
        Once submitted to a batch queue set status to active and set the job's
        name to some id from the batch system. Calling this method means the job
        has been submitted to the batch queue.
        """
        WMJob.submit(self, name = name)
        nameAction = self.daofactory(classname = "Jobs.UpdateName")
        nameAction.execute(self.id, name, conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        
        return

    def associateFiles(self):
        """
        _associateFiles_
        
        Update the wmbs_job_assoc table with the files in the inputFiles for the
        job.
        """
        files = WMJob.getFiles(self, type = "id")

        if len(files) > 0:
            addAction = self.daofactory(classname = "Jobs.AddFiles")
            addAction.execute(self.id, files, conn = self.getDBConn(),
                              transaction = self.existingTransaction())

        return

    def changeStatus(self, status):
        """
        _changeStatus_

        Change the status of the job.  The new status can be one of the
        following:
          ACTIVE, COMPLETE, FAILED
        """
        existingTransaction = self.beginTransaction()

        self.last_update = datetime.datetime.now()
        self.status = status.title()
        
        statusClass = "Jobs.%s" % self.status
        clearAction = self.daofactory(classname = "Jobs.ClearStatus")
        clearAction.execute(self.id, conn = self.getDBConn(),
                            transaction = self.existingTransaction())

        updateAction = self.daofactory(classname = statusClass)
        updateAction.execute(self.id, conn = self.getDBConn(),
                             transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)
        return

    def getStatus(self):
        """
        The status of the job
        
        The status of the jobs can be correctly inferred by comparing the start,
        complete and update times. The groups status is the sum of these 
        statuses.
        
        return: ACTIVE, COMPLETE, FAILED
        """
        statusAction = self.daofactory(classname = "Jobs.Status")
        ac, fa, cm = statusAction.execute(self.id, conn = self.getDBConn(),
                                          transaction = self.existingTransaction())
        
        # if it returns other than one some thing is wrong
        if cm == 1:
            return 'COMPLETE'
        if fa == 1:
            return 'FAILED'
        else:
            return 'ACTIVE'
