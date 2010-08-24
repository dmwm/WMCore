#!/usr/bin/env python
"""
_Job_

A job is owned by a subscription (which gives it it's workflow) and is 
associated to a (set of) file(s). The job interacts with its subscription
to acquire/complete/fail files. A job know's it's Workflow.

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

__revision__ = "$Id: Job.py,v 1.1 2008/07/07 09:43:20 metson Exp $"
__version__ = "$Revision: 1.1 $"

import datetime
from sets import Set

from WMCore.DataStructs.Job import Job as WMJob
from WMCore.WMBS.File import File
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.BusinessObject import BusinessObject

class Job(BusinessObject, WMJob):
    def __init__(self, subscription=None, files = Set(), id = -1, logger=None, dbfactory = None):
        """
        Subscription object is used to determine the workflow. 
        file_set is a set that contains the id's of all files the job should use.
        """
        BusinessObject.__init__(self, logger=subscription.logger, dbfactory=subscription.dbfactory)
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
        self.id, self.last_update = action.execute(subscription = self.subscription.id)
    
    def load(self):
        """
        Load the subscription and file id's from the database for a job of known id
        """
        pass
        
    def associateFiles(self):
        """
        update the wmbs_job_assoc table with the files in self.file_set
        """
        pass
        
    def resubmit(self):
        """
        Reset the file status to acquired for files associated to this job
        """
        pass
    
    def fail(self):
        """
        Job has failed, mark all files associated with it as failed
        """
        pass
    
    def complete(self):
        """
        Job has completed successfully, mark all files associated with it as complete
        """
        pass
        
        