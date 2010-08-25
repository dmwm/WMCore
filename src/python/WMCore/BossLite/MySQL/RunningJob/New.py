#!/usr/bin/env python
"""
_Create_

MySQL implementation of BossLite.Jobs.Create
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2010/05/09 20:07:58 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.Common.System import listToStr

class New(DBFormatter):
    sql = """INSERT INTO bl_runningjob (job_id, task_id, submission, state, scheduler, service,
                sched_attr, scheduler_id, scheduler_parent_id, status_scheduler, status,
                status_reason, destination, lb_timestamp, submission_time,
                scheduled_at_site, start_time, stop_time, stageout_time, getoutput_time,
                output_request_time, output_enqueue_time, getoutput_retry, output_dir, storage,
                lfn, application_return_code, wrapper_return_code, process_status, closed)
             VALUES (:jobId, :taskId, :submission, :state, :scheduler, :service, :schedulerAttributes,
                :schedulerId, :schedulerParentId, :statusScheduler, :status, :statusReason,
                :destination, :lbTimestamp, :submissionTime, :scheduledAtSite, :startTime,
                :stopTime, :stageOutTime, :getOutputTime, :outputRequestTime, :outputEnqueueTime,
                :getOutputRetry, :outputDirectory, :storage, :lfn, :applicationReturnCode,
                :wrapperReturnCode, :processStatus, :closed)
                """

    def preFormat(self, entry):
        """
        This method maps database fields with object dictionary and 
        it translate python List and timestamps in well formatted string
        """
        
        result = {}  
        
        #result['id']                    = entry['id']
        result['jobId']                 = entry['jobId']
        result['taskId']                = entry['taskId']
        result['submission']            = entry['submission']
        result['state']                 = entry['state']
        result['scheduler']             = entry['scheduler']
        result['service']               = entry['service']
        result['schedulerAttributes']   = entry['schedulerAttributes']
        result['schedulerId']           = entry['schedulerId']
        result['schedulerParentId']     = entry['schedulerParentId']
        result['statusScheduler']       = entry['statusScheduler']
        result['status']                = entry['status']
        result['statusReason']          = entry['statusReason']
        result['destination']           = entry['destination']
        result['lbTimestamp']           = entry['lbTimestamp']
        result['submissionTime']        = entry['submissionTime']
        result['scheduledAtSite']       = entry['scheduledAtSite']
        result['startTime']             = entry['startTime']
        result['stopTime']              = entry['stopTime']
        result['stageOutTime']          = entry['stageOutTime']
        result['getOutputTime']         = entry['getOutputTime']
        result['outputRequestTime']     = entry['outputRequestTime']
        result['outputEnqueueTime']     = entry['outputEnqueueTime']
        result['getOutputRetry']        = entry['getOutputRetry']
        result['outputDirectory']       = entry['outputDirectory']
        result['storage']               = listToStr(entry['storage'])
        result['lfn']                   = listToStr(entry['lfn'])
        result['applicationReturnCode'] = entry['applicationReturnCode']
        result['wrapperReturnCode']     = entry['wrapperReturnCode']
        result['processStatus']         = entry['processStatus']
        result['closed']                = entry['closed']
            
        return result
    
    def execute(self, binds, conn = None, transaction = False):
        """
        This assumes that you are passing in binds in the same format
        as BossLite.DbObjects.RunningJob, and that you already have an id.  It was
        too long a function for me to want to write in Perugia while
        parsing the binds
        """
        
        ppBinds = self.preFormat(binds)
        
        self.dbi.processData(self.sql, ppBinds, conn = conn,
                             transaction = transaction)
        return
    
