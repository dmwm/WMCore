#!/usr/bin/env python
#Turn off to many arguments
#pylint: disable-msg=R0913
#Turn off over riding built in id 
#pylint: disable-msg=W0622
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
             FirstEvent  INT(11),
             LastEvent   INT(11),
             FirstLumi   INT(11),
             LastLumi    INT(11),
             FirstRun    INT(11),
             LastRun     INT(11),
             start       INT(11),
             completed   INT(11),
             retries     INT(11),
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

Jobs are added to the WMBS database by their parent JobGroup, but are 
responsible for updating their state (and name).

TODO: Test/complete load
TODO: Load/Save Mask
"""

__revision__ = "$Id: Job.py,v 1.10 2008/10/28 18:59:26 metson Exp $"
__version__ = "$Revision: 1.10 $"

import datetime

from WMCore.Database.Transaction import Transaction
from WMCore.DataStructs.Job import Job as WMJob
from WMCore.DataStructs.Fileset import Fileset
from WMCore.WMBS.File import File
from WMCore.WMBS.BusinessObject import BusinessObject

class Job(BusinessObject, WMJob):
    """
    A job in WMBS
    """
    def __init__(self, name=None, files = None, id = -1, logger=None, dbfactory=None):
        """
        jobgroup object is used to determine the workflow. 
        file_set is a set that contains the id's of all files 
        the job should use.
        """
        WMJob.__init__(self, name=name, files = files)
        BusinessObject.__init__(self, 
                                logger=logger, 
                                dbfactory=dbfactory)
        
        self.id = id
        if self.id > 0:
            self.load()
            
    def create(self, group, trans = None):
        """
        Write the job to the database, connection and logger are picked up from 
        the JobGroup
        """
        
        newtrans = False
        if not trans:
            trans = Transaction(dbinterface = self.dbfactory.connect())
            newtrans = True
        try:    
            action = self.daofactory(classname='Jobs.New')
            self.id = action.execute(group.id, 
                                 self.name,
                                 conn = trans.conn, 
                                 transaction = True)
            if newtrans:
                trans.commit()
                print "created"
        except Exception, e:
            trans.rollback()
            raise e
                    
    def load(self):
        """
        Load the job and it's input from the database for a job of known id
        """
        # load the mask

    def getFiles(self, type='list'):
        """
        Get the files associated to the job
        """
        
        file_ids = self.daofactory(classname='Jobs.Load').execute(self.id)
        if file_ids == WMJob.getFiles(self, type='id'):
            return WMJob.getFiles(self, type)
        else:
            self.file_set = Fileset()
            for i in file_ids:
                file = File(id=i, logger=self.logger, dbfactory=self.dbfactory)
                file.load()
                self.file_set.addFile(file)
            return WMJob.getFiles(self, type)
            
    def submit(self, name=None):
        """
        Once submitted to a batch queue set status to active and set the job's
        name to some id from the batch system. Calling this method means the job
        has been submitted to the batch queue.
        """
        WMJob.submit(self, name=name)
        self.daofactory(classname='Jobs.UpdateName').execute(self.id, self.name)
                    
    def associateFiles(self, trans = None):
        """
        update the wmbs_job_assoc table with the files in self.file_set
        """
        newtrans = False
        if not trans:
            trans = Transaction(dbinterface = self.dbfactory.connect())
            newtrans = True
        try:
            files = self.file_set.getFiles(type='id')
            self.daofactory(classname='Jobs.AddFiles').execute(self.id, 
                                                           files,
                                                           conn = trans.conn, 
                                                           transaction = True)
            if newtrans:
                trans.commit()
        except Exception, e:
            trans.rollback()
            raise e
    
    def changeStatus(self, status):
        """
        possible states are: ACTIVE, FAILED, COMPLETE - files not in the state
        tables are considered new.
        """
        self.last_update = datetime.datetime.now()
        self.status = status
        trans = Transaction(dbinterface = self.dbfactory.connect())
        try:
            self.daofactory(classname='Jobs.ClearStatus').execute(self.id,
                                                       conn = trans.conn, 
                                                       transaction = True)
            self.daofactory(
                    classname='Jobs.%s' % self.status.title()).execute(self.id,
                                                       conn = trans.conn, 
                                                       transaction = True)
            trans.commit()
        except Exception, e:
            trans.rollback()
            raise e
        
        
