#!/usr/bin/env python
"""
_RunningJob_

Class for jobs that are running
"""

__version__ = "$Id: RunningJob.py,v 1.4 2010/04/26 12:20:14 spigafi Exp $"
__revision__ = "$Revision: 1.4 $"

# imports
import time
import logging

from WMCore.BossLite.DbObjects.DbObject import DbObject, dbTransaction
from WMCore.BossLite.Common.Exceptions import JobError, DbError


class RunningJob(DbObject):
    """
    RunningJob object
    """

    # fields on the object and their names on database
    fields = { 'id' : 'id',
               'jobId' : 'job_id',
               'taskId' : 'task_id',
               'submission' : 'submission',
               'state' : 'state',
               'scheduler' : 'scheduler',
               'service' : 'service',
               'schedulerAttributes' : 'sched_attr',
               'schedulerId' : 'scheduler_id',
               'schedulerParentId' : 'scheduler_parent_id',
               'statusScheduler' : 'status_scheduler',
               'status' : 'status',
               'statusReason' : 'status_reason',
               'destination' : 'destination',
               'lbTimestamp' : 'lb_timestamp',
               'submissionTime' : 'submission_time',
               'scheduledAtSite' : 'scheduled_at_site',
               'startTime' : 'start_time',
               'stopTime' : 'stop_time',
               'stageOutTime' : 'stageout_time',
               'getOutputTime' : 'getoutput_time',
               'outputRequestTime' : 'output_request_time',
               'outputEnqueueTime' : 'output_enqueue_time',
               'getOutputRetry' : 'getoutput_retry',
               'outputDirectory' : 'output_dir',
               'storage' : 'storage',
               'lfn' : 'lfn',
               'applicationReturnCode' : 'application_return_code',
               'wrapperReturnCode' : 'wrapper_return_code',
               'processStatus' : 'process_status',
               'closed' : 'closed'
             }

    # mapping between field names and database fields including superclass
    mapping = fields

    # default values for fields
    defaults = { 'id' : None,
                 'jobId' : None,
                 'taskId' : None,
                 'submission' : None,
                 'state' : None,
                 'scheduler' : None,
                 'service' : None,
                 'schedulerAttributes' : None,
                 'schedulerId' : None,
                 'schedulerParentId' : None,
                 'statusScheduler' : None,
                 'status' : None,
                 'statusReason' : None,
                 'destination' : None,
                 'lbTimestamp' : None,
                 'submissionTime' : None,
                 'scheduledAtSite' : None,
                 'startTime' : None,
                 'stopTime' : None,
                 'stageOutTime' : None,
                 'getOutputTime' : None,
                 'outputRequestTime' : None,
                 'outputEnqueueTime' : None,
                 'getOutputRetry' : 0,
                 'outputDirectory' : None,
                 'storage' : None,
                 'lfn' : '',
                 'applicationReturnCode' : None,
                 'wrapperReturnCode' : None,
                 'processStatus' : None,
                 'closed' : None
               }

    # database properties
    tableName = "bl_runningjob"
    tableIndex = ["taskId", "jobId", "submission"]
    timeFields = ['lbTimestamp', 'submissionTime', 'startTime', \
                  'scheduledAtSite' , 'stopTime', 'stageOutTime', \
                  'outputRequestTime', 'outputEnqueueTime', 'getOutputTime']
    # exception class
    exception = JobError

    ##########################################################################

    def __init__(self, parameters = {}):
        """
        initialize a RunningJob instance
        """

        # call super class init method
        DbObject.__init__(self, parameters)

        # set operational errors
        self.warnings = []
        self.errors = []

        # flag for scheduler interaction
        self.active = True

    ##########################################################################

    @dbTransaction
    def exists(self, noDB = False):
        """
        Am I in the database?
        """

        if noDB:
            if self.data['id'] > 0:
                return self.data['id']
            else:
                return False
        else:
            action = self.daofactory(classname = "RunningJob.Exists")
            id = action.execute(submission = self.data['submission'],
                                jobID = self.data['jobId'], 
                                taskID = self.data['taskId'],
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction )
                           
            if id:
                self.data['id'] = id
            self.existsInDataBase = True
            return id

    ##########################################################################

    @dbTransaction
    def create(self):
        """
        Create a new Running Job
        """

        action = self.daofactory(classname = "RunningJob.New")
        action.execute(binds = self.data,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)

        return

    ##########################################################################

    @dbTransaction
    def save(self):
        """
        Save the job
        """

        if not self.exists():
            self.create()
        else:
            action = self.daofactory(classname = "RunningJob.Save")
            action.execute(binds = self.data,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction)
                           

        self.existsInDataBase = True
        return

    ##########################################################################

    @dbTransaction
    def load(self, deep = True):
        """
        Load from the database

        """
        if self.data['id'] > 0:
            action = self.daofactory(classname = "RunningJob.LoadByID")
            result = action.execute(id = self.data['id'], 
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)
            
        elif (self.data['jobId'] and self.data['taskId'] and \
                self.data['submission']) :
            action = self.daofactory(classname = "RunningJob.LoadByParameters")
            result = action.execute(jobID = self.data['jobId'], 
                                    taskID = self.data['taskId'],
                                    submission = self.data['submission'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)
        else:
            # We have nothing
            return

        if result == []:
            # Then the job did not exist
            logging.error(
                "Attempted to load non-existant runningJob with parameters:\n %s" 
                        % (self.data) )
            return

        self.data.update(result[0])
        self.existsInDataBase = True
        
        return

    ##########################################################################

    @dbTransaction
    def remove(self):
        """
        remove job object from database
        """
        if not self.exists():
            logging.error("Cannot remove non-existant runningJob %s" 
                          % (self.data) )
            return 0

        action = self.daofactory(classname = "RunningJob.Delete")
        result = action.execute(value = self.data['id'],
                                column = 'id',
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction)


        # update status
        self.existsInDataBase = False

        # return number of entries removed
        return 1

    ##########################################################################

    def isError(self):
        """
        returns the status based on the self.errors list
        """

        return ( len( self.errors ) != 0 )
        
    ##########################################################################

    def update(self, db, deep = True):
        """
        update job information in database
        -> NEED TO PORTED ??? from Job, update is triggered by save() using 
            appropriate method 'updateRunningInstance' (NdFilippo)
        """

        # verify if the object exists in database
        if not self.existsInDataBase:

            # no, use save instead of update
            return self.save(db)

        # verify data is complete
        if not self.valid(['submission', 'jobId', 'taskId']):
            raise JobError("The following job instance cannot be updated," + \
                     " since it is not completely specified: %s" % self)

        # convert timestamp fields as required by mysql ('YYYY-MM-DD HH:MM:SS')
        for key in self.timeFields :
            try :
                self.data[key] = time.strftime("%Y-%m-%d %H:%M:%S", \
                                              time.gmtime(int(self.data[key])))
            # skip None and already formed strings
            except TypeError :
                pass
            except ValueError :
                pass

        # skip closed jobs?
        if deep :
            skipAttributes = None
        else :
            skipAttributes = {'closed' : 'Y'}

        # update it on database
        try:
            status = db.update(self, skipAttributes)
            # if status < 1:
            #     raise JobError("Cannot update job %s" % str(self))

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # return number of entries updated.
        # since (submission + jobId) is a key,it will be 0 or 1
        return status
