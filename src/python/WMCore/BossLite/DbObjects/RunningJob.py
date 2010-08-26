#!/usr/bin/env python
"""
_RunningJob_

Class for jobs that are running
"""

__version__ = "$Id: RunningJob.py,v 1.15 2010/06/28 19:12:03 spigafi Exp $"
__revision__ = "$Revision: 1.15 $"

from WMCore.BossLite.DbObjects.DbObject import DbObject, DbObjectDBFormatter

from WMCore.BossLite.Common.System import strToList, listToStr
from WMCore.BossLite.Common.System import encodeTimestamp, decodeTimestamp

from WMCore.BossLite.Common.Exceptions import JobError

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
                 'lbTimestamp' : 0,
                 'submissionTime' : 0,
                 'scheduledAtSite' : 0,
                 'startTime' : 0,
                 'stopTime' : 0,
                 'stageOutTime' : 0,
                 'getOutputTime' : 0,
                 'outputRequestTime' : 0,
                 'outputEnqueueTime' : 0,
                 'getOutputRetry' : 0,
                 'outputDirectory' : None,
                 'storage' : [],
                 'lfn' : [],
                 'applicationReturnCode' : None,
                 'wrapperReturnCode' : None,
                 'processStatus' : None,
                 'closed' : 'N'
               }
    
    # exception class
    exception = JobError
    
    # Time fields
    timeFields = ['lb_timestamp', 'submission_time', 'start_time', \
                  'scheduled_at_site' , 'stop_time', 'stageout_time', \
                  'output_request_time', 'output_enqueue_time', \
                  'getoutput_time']
    
    
    def __init__(self, parameters = None):
        """
        initialize a RunningJob instance
        """

        # call super class init method
        DbObject.__init__(self, parameters)

        self.warnings = []
        self.errors = []
        self.active = True
        self.existsInDataBase = False

        return
    
    
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
    
    
    def create(self, db):
        """
        Create a new Running Job
        """

        db.objCreate(self)
        
        self.existsInDataBase = True

        return
    
    
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
    
    
    def load(self, db, deep = True):
        """
        Load from the database
        """
        
        result = db.objLoad(self)
        
        if result == []:
            # raise exception?
            return 
        
        self.data.update(result[0])
        
        self.existsInDataBase = True
        
        return 
    
    
    def remove(self, db):
        """
        remove job object from database
        """
        
        if not self.existsInDataBase:
            return 0
        
        db.objRemove(self) 
        
        self.existsInDataBase = False

        # return number of entries removed
        return 1
    
    
    def isError(self):
        """
        returns the status based on the self.errors list
        """

        return ( len( self.errors ) != 0 )
    
    
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
        
        # result['id']                   = entry['id']
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
        result['lbTimestamp']           = \
                            encodeTimestamp(res['lbTimestamp'])
        result['submissionTime']        = \
                            encodeTimestamp(res['submissionTime'])
        result['scheduledAtSite']       = \
                            encodeTimestamp(res['scheduledAtSite'])
        result['startTime']             = \
                            encodeTimestamp(res['startTime'])
        result['stopTime']              = \
                            encodeTimestamp(res['stopTime'])
        result['stageOutTime']          = \
                            encodeTimestamp(res['stageOutTime'])
        result['getOutputTime']         = \
                            encodeTimestamp(res['getOutputTime'])
        result['outputRequestTime']     = \
                            encodeTimestamp(res['outputRequestTime'])
        result['outputEnqueueTime']     = \
                            encodeTimestamp(res['outputEnqueueTime'])
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
            result['lbTimestamp']           = \
                                decodeTimestamp(entry['lbtimestamp'])
            result['submissionTime']        = \
                                decodeTimestamp(entry['submissiontime'])
            result['scheduledAtSite']       = \
                                decodeTimestamp(entry['scheduledatsite'])
            result['startTime']             = \
                                decodeTimestamp(entry['starttime'])
            result['stopTime']              = \
                                decodeTimestamp(entry['stoptime'])
            result['stageOutTime']          = \
                                decodeTimestamp(entry['stageouttime'])
            result['getOutputTime']         = \
                                decodeTimestamp(entry['getoutputtime'])
            result['outputRequestTime']     = \
                                decodeTimestamp(entry['outputrequesttime'])
            result['outputEnqueueTime']     = \
                                decodeTimestamp(entry['outputenqueuetime'])
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
    