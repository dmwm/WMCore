#!/usr/bin/env python

"""
_Queries_

This module implements the mysql backend for the job  
emulator.

"""

__revision__ = \
    "$Id: Sites.py,v 1.1 2009/02/27 22:30:02 fvlingen Exp $"
__version__ = \
    "$Revision: 1.1 $"
__author__ = \
    "fvlingen@caltech.edu"

import threading

from WMCore.Database.DBFormatter import DBFormatter

class Sites(DBFormatter):

    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)

    def insertJob(self, jobID, jobType, jobSpecLocation, workerNode):
        """
        _insertJob_

        Add a job the the job emulator for processing given a unique ID
        and a type (processing, merge or cleanup).  That status and
        start time will be added automatically.

        """

        # uses the default value CURRENT_TIMESTAMP for time information
        sqlStr = """
INSERT INTO jem_job(job_id, job_type, start_time, status, job_spec, worker_node_id)
VALUES (  "%s", "%s", DEFAULT, "%s" ,"%s", "%s") 
""" % (jobID, jobType, "new", jobSpecLocation, workerNode)
        self.execute(sqlStr, {})

    def jobsByStatus(self, status):
        """
        _jobs_

        Returns a list of jobs in the Job Emulator database that
        have a particular status.  Each list item consists of the
        following tuple:

        (Job ID, Job Type (processing, merge or cleanup), Job Start Time,
        Job Status (new, finished, failed))
        """

        sqlStr = """
SELECT job_id, job_type, start_time, status, worker_node_id, job_spec FROM jem_job WHERE status = '%s' order by start_time
""" % status

        # return values are
        # job_id, job_type, start_time, status, worker_node_id
        # (x[0], x[1], x[2], x[3], x[4])
        result = self.execute(sqlStr, {})
        return self.format(result)

    def nodesByHost(self, hostID):
        """
        _nodesByHost

        Returns a node information. The item consists of the
        following tuple:

        (host_id, host_name, number_jobs)

        """

        sqlStr = """
SELECT host_id, host_name, number_jobs FROM jem_node WHERE host_id = %s
""" % str(hostID)
        result = self.execute(sqlStr, {})
        return self.formatOne(result)

    def jobsById(self, jobID):
        """
        _jobsById

        Returns a list of jobs in the Job Emulator database that
        have a particular job ID.  Each list item consists of the
        following tuple:

        (Job ID, Job Type (processing, merge or cleanup), Job Start Time,
        Job Status (new, finished, failed))

        """
        #check if the input is a list.
        if type(jobID) != list:
            jobID = [jobID]
        if len(jobID) == 0:
            return []
        if len(jobID) == 1:       
            sqlStr = """
SELECT job_id,job_type,start_time,status,worker_node_id,job_spec FROM jem_job WHERE job_id = '%s' order by start_time
""" % jobID[0]
        else:
            sqlStr = """
SELECT job_id,job_type,start_time,status,worker_node_id,job_spec FROM jem_job WHERE job_id IN  %s order by start_time
""" % str(tuple(jobID)) 
        result = self.execute(sqlStr, {})
        return self.format(result)

    def reset(self):
        """
        Resets the job emulator database info.
        """

        sqlStr = """
DELETE FROM jem_job
"""
        self.execute(sqlStr, {})
        sqlStr = """
DELETE FROM jem_node
"""
        self.execute(sqlStr, {})

    def removeJob(self, jobID):
        """
        _removeJob_

        Remove any job from the job_emulator table that has a
        particular job ID.

        """
        if type(jobID) != list:
            jobID = [jobID]
        if len(jobID) == 0:
            return []
        if len(jobID) == 1:
            sqlStr = """
DELETE FROM jem_job WHERE job_id = '%s'
""" % jobID[0]
        else:
            sqlStr = """
DELETE FROM jem_job WHERE job_id IN %s
""" % str(tuple(jobID))
        self.execute(sqlStr, {})

    def updateJobStatus(self, jobID, status):
        """
        _updateJobStatus_

        Change the status of a job with a particular job ID. Status
        can be either new, finished or failed.

        """

        sqlStr = """
UPDATE jem_job SET status = '%s' WHERE job_id = '%s'
""" % (status, jobID)
        self.execute(sqlStr, {})

    def updateJobAlloction(self, jobID, nodeID):
        """
        _updateJobAlloction_

        update the worker node information by given job id on job_emulator table

        """

        sqlStr = """
UPDATE jem_job SET worker_node_id= %d WHERE job_id = '%s'
""" % (nodeID, jobID)
        self.execute(sqlStr, {})

    def insertWorkerNode(self, nodeName, jobCount=0):
        """
        _insertWorkerNode_
        insert worker node info to jem_node table
        """
        sqlStr = """
INSERT INTO jem_node(host_name, number_jobs)
VALUES ('%s', %d) """ % (nodeName, jobCount)
        self.execute(sqlStr, {})

    def increaseJobCount(self, jobID):
        """
        _increaseJobCount_

        increase job count by 1 on given job id
        """
        sqlStr = \
        """
UPDATE jem_node SET number_jobs = number_jobs + 1
WHERE host_id =
(SELECT worker_node_id FROM jem_job WHERE job_id = '%s')
""" % jobID
        self.execute(sqlStr, {})

    def increaseJobCountByNodeID(self, nodeID):
        """
        _increaseJobCountByNodeID_

        increase job count by 1 on given node id
        """
        sqlStr = """
UPDATE jem_node SET number_jobs = number_jobs + 1
WHERE host_id = %d
"""% nodeID
        self.execute(sqlStr, {})

    def decreaseJobCount(self, jobID):
        """
        _decreaseJobCount_

        decrease job count by 1 on given job id
        """
        sqlStr = \
        """
UPDATE jem_node SET number_jobs = number_jobs - 1
WHERE host_id =
(SELECT worker_node_id FROM jem_job WHERE job_id = '%s')
""" % jobID
        self.execute(sqlStr, {})

    def decreaseJobCountByNodeID(self, nodeID):
        """
        _decreaseJobCountByNodeID_

        decrease job count by 1 on given node id
        """
        sqlStr = """
UPDATE jem_node SET number_jobs = number_jobs - 1
WHERE host_id = %d
""" % nodeID
        self.execute(sqlStr, {})

    def selecNodeWithLeastJobs(self):
        """
        _selecNodeWithLeastJobs_

        Returns one node from jobEM_node_info table which contain the least number of running jobs
        (HostID, HostName, number_jobs)

        """

        sqlStr = """
SELECT host_id,host_name,number_jobs FROM jem_node order by number_jobs ASC, host_id
"""
        result = self.execute(sqlStr, {})
        return self.formatOne(result)

    def updateJobAlloction(self, jobID, nodeID):
        """
        _updateJobAlloction_

        update the worker node information by given job id on jem_job table

        """

        sqlStr = """
UPDATE jem_job SET worker_node_id= %d WHERE job_id = '%s'
""" % (nodeID, jobID)
        self.execute(sqlStr, {})

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

