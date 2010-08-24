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

__revision__ = "$Id: Job.py,v 1.13 2008/12/10 22:27:15 sryu Exp $"
__version__ = "$Revision: 1.13 $"

import datetime
import threading

from WMCore.Database.Transaction import Transaction
from WMCore.DataStructs.Job import Job as WMJob
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File

class Job(WMJob):
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
        WMJob.__init__(self, name = name, files = files)

        myThread = threading.currentThread()
        self.logger = myThread.logger
        self.dialect = myThread.dialect
        self.dbi = myThread.dbi
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.logger,
                                     dbinterface = self.dbi)
        
        self.id = id
        return
            
    def create(self, group, trans = None):
        """
        _create_
        
        Write the job to the database.  If no database transaction is passed
        to this method one will be created and commited.
        """
        self.job_group = group.id
        
        jobAction = self.daofactory(classname = "Jobs.New")
        self.id = jobAction.execute(self.job_group, self.name)
        maskAction = self.daofactory(classname = "Masks.New")
        maskAction.execute(jobid = self.id)
        self.save()

    def delete(self):
        """
        Remove a job from WMBS
        """
        self.daofactory(classname='Jobs.Delete').execute(id = self.id)

    def save(self):
        """
        _save_

        Flush all changes that have been made to the job to the database.
        """
        self.daofactory(classname = "Jobs.Save").execute(self.id,
                                                         self.job_group,
                                                         self.name)

        maskAction = self.daofactory(classname = "Masks.Save")
        maskAction.execute(jobid = self.id, mask = self.mask)

        self.associateFiles()
        return

    def exists(self):
        """
        _exists_

        Does a job exist with this name.
        """
        # if id is available check with id
        if self.id != -1:
            action = self.daofactory(classname='Jobs.ExistsByID')
            return action.execute(id = self.id)
        else:
            action = self.daofactory(classname='Jobs.Exists')
            return action.execute(name = self.name)
                
    def load(self, method = "Jobs.LoadFromID"):
        """
        _load_
        
        Given the job's ID load all information from the database including any
        files that may be associated with the job and it's mask.
        """
        if method == "Jobs.LoadFromID":
            metaData = self.daofactory(classname = "Jobs.LoadFromID").execute(self.id)
        elif method == "Jobs.LoadFromName":
            metaData = self.daofactory(classname = "Jobs.LoadFromName").execute(self.name)
        else:
            self.logger.error("Unknown load method: %s" % method)
            return

        self.id = metaData["ID"]
        self.name = metaData["NAME"]
        self.last_update = metaData["LAST_UPDATE"]
        self.job_group = metaData["JOBGROUP"]

        jobMask = self.daofactory(classname = "Masks.Load").execute(self.id)
        for keyName in self.mask.keys():
            self.mask[keyName] = jobMask.get(keyName, None)

        fileIDs = self.daofactory(classname = "Jobs.LoadFiles").execute(self.id)

        self.file_set = Fileset()
        for fileID in fileIDs:
            newFile = File(id = fileID)
            newFile.load()
            self.file_set.addFile(newFile)

        return

    def getFiles(self, type = "list"):
        """
        _getFiles_

        Retrieve the files that are associated with this job.  This checks to
        make sure that the files in this object's file_set match the files that
        are associated to the job in the database.  If the two do not match then
        this object's fileset will be re-populated with files from the database.
        """
        fileIDs = self.daofactory(classname = "Jobs.LoadFiles").execute(self.id)
        
        if fileIDs != WMJob.getFiles(self, type = "id"):
            self.load()

        return WMJob.getFiles(self, type)
            
    def submit(self, name = None):
        """
        _submit_
        
        Once submitted to a batch queue set status to active and set the job's
        name to some id from the batch system. Calling this method means the job
        has been submitted to the batch queue.
        """
        WMJob.submit(self, name = name)
        self.daofactory(classname = "Jobs.UpdateName").execute(self.id, name)
                    
    def associateFiles(self):
        """
        _associateFiles_
        
        Update the wmbs_job_assoc table with the files in the file_set for the
        job.
        """
        files = self.file_set.getFiles(type = "id")
        self.daofactory(classname = "Jobs.AddFiles").execute(self.id, files)

    def changeStatus(self, status):
        """
        _changeStatus_

        Change the status of the job.  The new status can be one of the
        following:
          ACTIVE, COMPLETE, FAILED
        """
        self.last_update = datetime.datetime.now()
        self.status = status.title()
        
        trans = Transaction(self.dbi)
        try:
            statusClass = "Jobs.%s" % self.status
            self.daofactory(classname = "Jobs.ClearStatus").execute(self.id,
                                                                    conn = trans.conn,
                                                                    transaction = True)
            self.daofactory(classname = statusClass).execute(self.id,
                                                             conn = trans.conn,
                                                             transaction = True)
            trans.commit()
        except Exception, e:
            trans.rollback()
            raise e
