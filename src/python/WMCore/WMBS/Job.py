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

__revision__ = "$Id: Job.py,v 1.23 2009/02/12 21:23:30 sryu Exp $"
__version__ = "$Revision: 1.23 $"

import datetime
from sets import Set

from WMCore.DataStructs.Job import Job as WMJob
from WMCore.DataStructs.Fileset import Fileset
from WMCore.WMBS.File import File
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
        file_set is a set that contains the id's of all files 
        the job should use.
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
        self.job_group = group.id

        if self.name == None:
            self.name = makeUUID()

        jobAction = self.daofactory(classname = "Jobs.New")
        jobAction.execute(self.job_group, self.name,
                          conn = self.getWriteDBConn(),
                          transaction = self.existingTransaction())

        self.load()
        
        maskAction = self.daofactory(classname = "Masks.New")
        maskAction.execute(jobid = self.id, conn = self.getWriteDBConn(),
                           transaction = self.existingTransaction())
        self.save()
        self.commitIfNew()
        return
        
    def delete(self):
        """
        Remove a job from WMBS
        """
        deleteAction = self.daofactory(classname='Jobs.Delete')
        deleteAction.execute(id = self.id, conn = self.getWriteDBConn(),
                              transaction = self.existingTransaction())
        self.commitIfNew()
        return

    def save(self):
        """
        _save_

        Flush all changes that have been made to the job to the database.
        """
        saveAction = self.daofactory(classname = "Jobs.Save")
        saveAction.execute(self.id, self.job_group, self.name,
                           conn = self.getWriteDBConn(),
                           transaction = self.existingTransaction())

        maskAction = self.daofactory(classname = "Masks.Save")
        maskAction.execute(jobid = self.id, mask = self.mask,
                           conn = self.getWriteDBConn(),
                           transaction = self.existingTransaction())

        self.associateFiles()
        self.commitIfNew()
        return

    def exists(self):
        """
        _exists_

        Does a job exist with this name.
        """
        if self.id != -1:
            action = self.daofactory(classname='Jobs.ExistsByID')
            return action.execute(id = self.id, conn = self.getReadDBConn(),
                                  transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname='Jobs.Exists')
            return action.execute(name = self.name, conn = self.getReadDBConn(),
                                  transaction = self.existingTransaction())
                
    def load(self):
        """
        _load_

        Load the job's name, id and jobgroup from the database.  Either the ID
        or the name must be set before this is called.
        """
        if self.id > 0:
            loadAction = self.daofactory(classname = "Jobs.LoadFromID")
            results = loadAction.execute(self.id, conn = self.getReadDBConn(),
                                         transaction = self.existingTransaction())
        else:
            loadAction = self.daofactory(classname = "Jobs.LoadFromName")
            results = loadAction.execute(self.name, conn = self.getReadDBConn(),
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
        if self.id < 0 or self.name == None:
            self.load()

        jobMaskAction = self.daofactory(classname = "Masks.Load")
        jobMask = jobMaskAction.execute(self.id, conn = self.getReadDBConn(),
                                        transaction = self.existingTransaction())

        self.mask.update(jobMask)

        fileAction = self.daofactory(classname = "Jobs.LoadFiles")
        files = fileAction.execute(self.id, conn = self.getReadDBConn(),
                                     transaction = self.existingTransaction())

        self.file_set = Fileset()
        for file in files:
            newFile = File(id = file["id"])
            newFile.loadData(parentage = 0)
            self.file_set.addFile(newFile)

        self.file_set.commit()
        return

    def getFiles(self, type = "list"):
        """
        _getFiles_

        Retrieve the files that are associated with this job.  If the id of the
        job is -1 this will skip loading from the database and return whatever
        files currently exist in the object.  If the id has been set this will
        make sure that the files in this object's file_set match the files that
        are associated to the job in the database.  If the two do not match then
        this object's fileset will be re-populated with files from the database.
        """
        if self.id < 0:
            return WMJob.getFiles(self, type)
    
        fileIDs = self.getFileIDs()
        if fileIDs == None:
            return None
        
        if fileIDs != WMJob.getFiles(self, type = "id"):
            self.loadData()

        return WMJob.getFiles(self, type)

    def getFileIDs(self, type="list"):
        """
        _getFileIDs_

        Retrieve a list of the file IDs that are associated with this job.  The
        ID of the job must be set before this is called.
        """
        if self.id < 0:
            self.logger.error("Need to set job id before files can be retrieved")
            return None
        
        fileAction = self.daofactory(classname = "Jobs.LoadFiles")
        fileIDs = fileAction.execute(self.id, conn = self.getReadDBConn(),
                                     transaction = self.existingTransaction())        
        if type == "list":
            fileIDList = []
            for id in fileIDs:
                fileIDList.append(id["id"])
            return fileIDList
        elif type == "dict":
            return fileIDs
        
    
    def addOutput(self, file):
        """
        add output file to job groups groupoutput file set.
        then, update the file parentage
        also add run-lumi information
        take wmbs file object as a argument (doesn't have to be loaded). 
        """ 
        # this requires Jobgroup to call addOutput to commit to database
        WMJob.addOutput(self, file)
        
        # This dosen't require JobGreoup to call addOutput separately but need 
        # to import JobGroup with in the job which might be worse than calling 
        # JobGroup.addOutput after Job.addOutput
#        from WMCore.WMBS.JobGroup import JobGroup
#        
#        jobGroup = JobGroup(id=self.job_group)
#        jobGroup.load()
#        jobGroup.groupoutput.addFile(file)
#        jobGroup.groupoutput.commit()
        
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
                
         # if the performance is the problem use following to access directly 
#        # DOA. Still need to add the code to AddRunLimi
#        #set the parentage
#        inputFileIDs = self.getFileIDs()
#        
#        fileAction = self.daofactory(classname = "Files.Heritage")
#        
#        for inputFileID in inputFileIDs:
#            fileAction.execute(parent=inputFileID, child=file["id"],
#                               conn = self.getReadDBConn(),
#                               transaction = self.existingTransaction())
#            
           
    def submit(self, name = None):
        """
        _submit_
        
        Once submitted to a batch queue set status to active and set the job's
        name to some id from the batch system. Calling this method means the job
        has been submitted to the batch queue.
        """
        WMJob.submit(self, name = name)
        nameAction = self.daofactory(classname = "Jobs.UpdateName")
        nameAction.execute(self.id, name, conn = self.getWriteDBConn(),
                           transaction = self.existingTransaction())
        self.commitIfNew()
        return

    def associateFiles(self):
        """
        _associateFiles_
        
        Update the wmbs_job_assoc table with the files in the file_set for the
        job.
        """
        files = self.file_set.getFiles(type = "id")

        if len(files) > 0:
            addAction = self.daofactory(classname = "Jobs.AddFiles")
            addAction.execute(self.id, files, conn = self.getWriteDBConn(),
                              transaction = self.existingTransaction())
            self.commitIfNew()

        return

    def changeStatus(self, status):
        """
        _changeStatus_

        Change the status of the job.  The new status can be one of the
        following:
          ACTIVE, COMPLETE, FAILED
        """
        self.last_update = datetime.datetime.now()
        self.status = status.title()
        
        statusClass = "Jobs.%s" % self.status
        clearAction = self.daofactory(classname = "Jobs.ClearStatus")
        clearAction.execute(self.id, conn = self.getWriteDBConn(),
                            transaction = self.existingTransaction())

        updateAction = self.daofactory(classname = statusClass)
        updateAction.execute(self.id, conn = self.getWriteDBConn(),
                             transaction = self.existingTransaction())

        self.commitIfNew()
        return
