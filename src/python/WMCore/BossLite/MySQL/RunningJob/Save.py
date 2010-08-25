#!/usr/bin/env python
"""
_Save_

MySQL implementation of BossLite.Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.2 2010/05/09 20:07:58 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.Common.System import listToStr

class Save(DBFormatter):
    sql = """UPDATE bl_runningjob
              SET job_id = :jobId, task_id = :taskId, submission = :submission,
                state = :state, scheduler = :scheduler, service = :service,
                sched_attr = :schedulerAttributes, scheduler_id = :schedulerId,
                scheduler_parent_id = :schedulerParentId, status_scheduler = :statusScheduler,
                status = :status, status_reason = :statusReason, destination = :destination,
                lb_timestamp = :lbTimestamp, submission_time = :submissionTime,
                scheduled_at_site = :scheduledAtSite, start_time = :startTime, stop_time = :stopTime,
                stageout_time = :stageOutTime, getoutput_time = :getOutputTime, 
                output_request_time = :outputRequestTime, output_enqueue_time = :outputEnqueueTime,
                getoutput_retry = :getOutputRetry, output_dir = :outputDirectory, storage = :storage,
                lfn = :lfn, application_return_code = :applicationReturnCode,
                wrapper_return_code = :wrapperReturnCode, process_status = :processStatus,
                closed = :closed
             WHERE job_id = :jobId
                AND task_id = :taskId
                AND submission = :submission
                """

    def preFormat(self, entry):
        """
        This method maps database fields with object dictionary and 
        it translate python List and timestamps in well formatted string
        """
        
        result = {}  
        
        result['id']                    = entry['id']
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
    
