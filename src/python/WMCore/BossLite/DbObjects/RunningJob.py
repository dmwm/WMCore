#!/usr/bin/env python
"""
_RunningJob_

Class for jobs that are running
"""

__version__ = "$Id: RunningJob.py,v 1.10 2010/05/11 10:47:55 spigafi Exp $"
__revision__ = "$Revision: 1.10 $"

# imports
# import logging

from WMCore.BossLite.DbObjects.DbObject import DbObject, DbObjectDBFormatter

from WMCore.BossLite.Common.Exceptions import JobError
from WMCore.BossLite.Common.System import strToList, listToStr, strToTimestamp

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
                 'storage' : [],
                 'lfn' : [],
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

    def __init__(self, parameters = None):
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
        self.existsInDataBase = False

    ##########################################################################

    def exists(self, db, noDB = False):
        """
        Am I in the database?
        """

        if noDB:
            if self.data['id'] > 0:
                return self.data['id']
            else:
                return False
        else:
            tmpId = db.objExists(self)
                        
            if tmpId:
                self.data['id'] = tmpId
            
            return tmpId

    ##########################################################################

    def create(self, db):
        """
        Create a new Running Job
        """

        db.objCreate(self)
        
        # "if self.exists(db)" is not necessary because to save & create 
        # a valid RunningJob (jobID, taskID, submission) must be valid!  
        self.existsInDataBase = True

    ##########################################################################

    def save(self, db, deep = True):
        """
        Save the job
        """

        if not self.existsInDataBase:
            self.create(db)
        else:
            db.objSave(self)               

        self.existsInDataBase = True
        
        return

    ##########################################################################

    def load(self, db, deep = True):
        """
        Load from the database
        """
        
        result = db.objLoad(self)
        
        if result == []:
            # Then the job did not exist
            # no exception will raise?
            
            # is this message useful? TEMPORARY SUPPRESSED
            #logging.error(
            # "Attempted to load non-existant runningJob with parameters:\n %s" 
            #            % (self.data) )
            return 
        
        self.data.update(result[0])
        
        # consistency?
        self.existsInDataBase = True
        
        return 

    ##########################################################################

    def remove(self, db):
        """
        remove job object from database
        """
        
        if not self.existsInDataBase:
            # need to check!
            # logging.error("Cannot remove from DB non-existant runningJob %s" 
            #              % (self.data) )
            return 0
        
        db.objRemove(self) 
        
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
        """

        return self.save(db, deep)


class RunningJobDBFormatter(DbObjectDBFormatter):
    """
    RunningJobDBFormatter
    """

    def preFormat(self, res):
        """
        It maps database fields with object dictionary and it translate python 
        List and timestamps in well formatted string. This is useful for any 
        kind of database engine!
        """
        
        result = {}  
        
        # result['id']                    = entry['id']
        result['jobId']                 = res['jobId']
        result['taskId']                = res['taskId']
        result['submission']            = res['submission']
        result['state']                 = res['state']
        result['scheduler']             = res['scheduler']
        result['service']               = res['service']
        result['schedulerAttributes']   = res['schedulerAttributes']
        result['schedulerId']           = res['schedulerId']
        result['schedulerParentId']     = res['schedulerParentId']
        result['statusScheduler']       = res['statusScheduler']
        result['status']                = res['status']
        result['statusReason']          = res['statusReason']
        result['destination']           = res['destination']
        result['lbTimestamp']           = strToTimestamp(res['lbTimestamp'])
        result['submissionTime']        = strToTimestamp(res['submissionTime'])
        result['scheduledAtSite']       = \
                                    strToTimestamp(res['scheduledAtSite'])
        result['startTime']             = strToTimestamp(res['startTime'])
        result['stopTime']              = strToTimestamp(res['stopTime'])
        result['stageOutTime']          = strToTimestamp(res['stageOutTime'])
        result['getOutputTime']         = strToTimestamp(res['getOutputTime'])
        result['outputRequestTime']     = \
                                    strToTimestamp(res['outputRequestTime'])
        result['outputEnqueueTime']     = \
                                    strToTimestamp(res['outputEnqueueTime'])
        result['getOutputRetry']        = res['getOutputRetry']
        result['outputDirectory']       = res['outputDirectory']
        result['storage']               = listToStr(res['storage'])
        result['lfn']                   = listToStr(res['lfn'])
        result['applicationReturnCode'] = res['applicationReturnCode']
        result['wrapperReturnCode']     = res['wrapperReturnCode']
        result['processStatus']         = res['processStatus']
        result['closed']                = res['closed']
        
        return result
    
    def postFormat(self, res):
        """
        Format the results into the right output. This is useful for any 
        kind of database engine!
        """
        
        final = []
        for entry in res:
            result = {}
            result['id']                    = entry['id']
            result['jobId']                 = entry['jobid']
            result['taskId']                = entry['taskid']
            result['submission']            = entry['submission']
            result['state']                 = entry['state']
            result['scheduler']             = entry['scheduler']
            result['service']               = entry['service']
            result['schedulerAttributes']   = entry['schedulerattributes']
            result['schedulerId']           = entry['schedulerid']
            result['schedulerParentId']     = entry['schedulerparentid']
            result['statusScheduler']       = entry['statusscheduler']
            result['status']                = entry['status']
            result['statusReason']          = entry['statusreason']
            result['destination']           = entry['destination']
            result['lbTimestamp']           = entry['lbtimestamp']
            result['submissionTime']        = entry['submissiontime']
            result['scheduledAtSite']       = entry['scheduledatsite']
            result['startTime']             = entry['starttime']
            result['stopTime']              = entry['stoptime']
            result['stageOutTime']          = entry['stageouttime']
            result['getOutputTime']         = entry['getoutputtime']
            result['outputRequestTime']     = entry['outputrequesttime']
            result['outputEnqueueTime']     = entry['outputenqueuetime']
            result['getOutputRetry']        = entry['getoutputretry']
            result['outputDirectory']       = entry['outputdirectory']
            result['storage']               = strToList(entry['storage'])
            result['lfn']                   = strToList(entry['lfn'])
            result['applicationReturnCode'] = entry['applicationreturncode']
            result['wrapperReturnCode']     = entry['wrapperreturncode']
            result['processStatus']         = entry['processstatus']
            result['closed']                = entry['closed']
            final.append(result)

        return final
    