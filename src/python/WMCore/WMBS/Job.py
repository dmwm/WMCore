#!/usr/bin/env python
"""
_Job_

A job is owned by a subscription (which gives it it's workflow) and is 
associated to a (set of) file(s). The job interacts with its subscription
to acquire/complete/fail files. A job know's it's Workflow via it's 
subscription. A job is meaningless without a subscription.

CREATE TABLE wmbs_job (
    id           INT(11) NOT NULL AUTO_INCREMENT,
    subscription INT(11) NOT NULL,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                    ON DELETE CASCADE)

CREATE TABLE wmbs_job_assoc (
    job    INT(11) NOT NULL,
    file   INT(11) NOT NULL,
    FOREIGN KEY (job) REFERENCES wmbs_job(id)
                    ON DELETE CASCADE,
    FOREIGN KEY (file) REFERENCES wmbs_file(id)
                    ON DELETE CASCADE)

"""

__revision__ = "$Id: Job.py,v 1.5 2008/09/19 16:29:44 metson Exp $"
__version__ = "$Revision: 1.5 $"

import datetime
from sets import Set

from WMCore.DataStructs.Job import Job as WMJob
from WMCore.DataStructs.Fileset import Fileset
from WMCore.WMBS.File import File
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.BusinessObject import BusinessObject

class Job(BusinessObject, WMJob):
    def __init__(self, subscription=None, files = None, id = -1):
        """
        Subscription object is used to determine the workflow. 
        file_set is a set that contains the id's of all files 
        the job should use.
        """
        BusinessObject.__init__(self, 
                                logger=subscription.logger, 
                                dbfactory=subscription.dbfactory)
        WMJob.__init__(self, subscription=subscription, files = files)
        
        self.id = id
        if self.id == -1:
            self.create()
        else:
            self.load()
            
    def create(self):
        """
        Add a new row to wmbs_job
        """
        action = self.daofactory(classname="Jobs.New")
        self.id, self.last_update = \
                action.execute(subscription = self.subscription.id)
        self.load()
    
    def load(self):
        """
        Load the subscription and file id's from 
        the database for a job of known id
        """
        self.file_set = Fileset()
        file_ids = self.daofactory(classname='Jobs.Load').execute(self.id)
        for i in file_ids:
            file = File(id=i, logger=self.logger, dbfactory=self.dbfactory)
            file.load()
            self.file_set.addFile(file)
            
    def associateFiles(self):
        """
        update the wmbs_job_assoc table with the files in self.file_set
        """
        def getFileId(file):
             return file.dict["id"]
        files = map(getFileId, self.file_set.listFiles())
        self.daofactory(classname='Jobs.AddFiles').execute(self.id, files)
    
    def resubmit(self):
        """
        Reset the file status to acquired for files associated to this job
        """
        pass
    
    def fail(self):
        """
        Job has failed, mark all files associated with it as failed
        """
        self.subscription.failFiles(self.file_set.listFiles())
    
    def complete(self):
        """
        Job has completed successfully, mark all files associated 
        with it as complete
        """
        self.subscription.completeFiles(self.file_set.listFiles())
        
        