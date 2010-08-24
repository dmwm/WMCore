#!/usr/bin/env python

"""
_PlaceboTrackerDB_

This module implements the mysql backend for the job  
emulator placebo tracker db that is used in in-situ mode.

"""

__revision__ = \
    "$Id: PlaceboTrackerDB.py,v 1.1 2009/02/27 22:30:02 fvlingen Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class PlaceboTrackerDB(DBFormatter):

    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

    def insertJob(self, jobID):
        """
        _insertJob_

        Add a job to the placebo tracker db 
        """

        sqlStr = """
INSERT INTO jem_placebo_tracker_db(job_id, status)
VALUES (  "%s", "submitted")
""" % (jobID)
        self.execute(sqlStr, {})

    def reset(self):
        """
        Resets the job emulator database info.
        """

        sqlStr = """
DELETE FROM jem_placebo_tracker_db
"""
        self.execute(sqlStr, {})

    def changeState(self, jobID, state):
        """
        Change state of job or many jobs
        """
        if type(jobID) != list:
            sqlStr = """
UPDATE jem_placebo_tracker_db SET status = '%s' 
WHERE job_id = '%s'
""" %(state, jobID)
        else:
            if len(jobID) == 0:
                return 
            sqlStr = """ 
UPDATE jem_placebo_tracker_db SET status = '%s' 
WHERE job_id IN %s
""" %(state, str(tuple(jobID)))
        self.execute(sqlStr, {})

    def jobsByState(self, status):
        """
        Returns jobs by a particular state.
        """
        sqlStr = """
SELECT job_id FROM jem_placebo_tracker_db WHERE
status="%s" """ %(status)
        result = self.execute(sqlStr, {})
        result = self.format(result)
        # make it an array:
        if len(result) == 0:
            return []
        l = []
        for i in result:
            l.append(i[0])
        return l

    # following three are interfaces similar to the one
    # used in the real tracker db api.
    def jobRunning(self, jobID):
        self.changeState(jobID, 'running')

    def jobComplete(self, jobID):
        self.changeState(jobID, 'completed')

    def jobFailed(self, jobID):
        self.changeState(jobID, 'failed')

    def execute(self, sqlStr, args):
        """"
        __execute__
        Executes the queries by getting the current transaction
        and dbinterface object that is stored in the reserved words of
        the thread it operates in.
        """
        myThread = threading.currentThread()
        currentTransaction = myThread.transaction
        return currentTransaction.processData(sqlStr, args)

