#!/usr/bin/env python
"""
_RunningJob_

Class for jobs that are running
"""

__version__ = "$Id: RunningJob.py,v 1.2 2010/03/30 15:31:53 mnorman Exp $"
__revision__ = "$Revision: 1.2 $"

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
                                jobID = self.data['jobId'], taskID = self.data['taskId'],
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction)
                           
            if id:
                self.data['id'] = id
            self.existsInDataBase = True
            return id

    #############################################################


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

    ###############################################################

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


    ##############################################################

    @dbTransaction
    def load(self):
        """
        Load from the database

        """
        if self.data['id'] > 0:
            action = self.daofactory(classname = "RunningJob.LoadByID")
            result = action.execute(id = self.data['id'], 
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)
            
        elif self.data['jobId'] and self.data['taskId'] and self.data['submission']:
            action = self.daofactory(classname = "RunningJob.LoadByParameters")
            result = action.execute(jobID = self.data['jobId'], taskID = self.data['taskId'],
                                    submission = self.data['submission'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction)
        else:
            # We have nothing
            return
            

        if result == []:
            # Then the job did not exist
            logging.error('Attempted to load non-existant runningJob with parameters:\n %s' %(self.data))
            return

        self.data.update(result[0])
        self.existsInDataBase = True
        return



    ###################################################################
    #  This is where I stopped doing work
    ###################################################################


    def isError(self):
        """
        returns the status based on the self.errors list
        """

        return ( len( self.errors ) != 0 )
        

    ##########################################################################

#    def save(self, db):
#        """
#        save running job object in database. checking that static information
#        is automatically performed due to database constraints
#        """
#
#        # verify data is complete
#        if not self.valid(['submission', 'jobId', 'taskId']):
#            raise JobError("The following job instance cannot be saved," + \
#                     " since it is not completely specified: %s" % self)
#
#        # insert running job
#        try:
#
#            # create entry in database
#            status = db.insert(self)
#            if status != 1:
#                raise JobError("Cannot insert running job %s" % str(self))
#
#        # database error
#        except DbError, msg:
#            raise JobError(str(msg))
#
#        # update status
#        self.existsInDataBase = True
#
#        return status

    ##########################################################################

    def remove(self, db):
        """
        remove job object from database
        """

        # verify data is complete
        if not self.valid(['submission', 'jobId']):
            raise JobError("The following job instance cannot be removed," + \
                     " since it is not completely specified: %s" % self)

        # remove from database
        try:
            status = db.delete(self)
            if status < 1:
                raise JobError("Cannot remove running job %s" % str(self))

        # database error
        except DbError, msg:
            raise JobError(str(msg))

        # update status
        self.existsInDataBase = False

        # return number of entries removed
        return status

    ##########################################################################

    def update(self, db, deep = True):
        """
        update job information in database
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

   ##########################################################################

#    def load(self, db, deep = True):
#        """
#        load information from database
#        """
#
#       # verify data is complete
#        if not self.valid(['name']):
#            raise JobError("The following running job instance cannot be" + \
#                     " loaded since it is not completely specified: %s" % self)
#
#        # get information from database based on template object
#        try:
#            objects = db.select(self)
#
#        # database error
#        except DbError, msg:
#            raise JobError(str(msg))
#
#        # since required data is a key, it should be a single object list
#        if len(objects) == 0:
#            raise JobError("No running job instances corresponds to the," + \
#                     " template specified: %s" % self)
#
#        if len(objects) > 1:
#            raise JobError("Multiple running job instances corresponds to" + \
#                     " the template specified: %s" % self)
#
#        # copy fields
#        for key in self.fields:
#            self.data[key] = objects[0][key]
#
#        # update status
#        self.existsInDataBase = True

