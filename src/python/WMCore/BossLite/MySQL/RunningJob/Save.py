#!/usr/bin/env python
"""
_Save_

MySQL implementation of BossLite.RunningJob.Save
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.RunningJob import RunningJobDBFormatter

class Save(DBFormatter):
    """
    BossLite.RunningJob.Save
    """
    
    sql = """UPDATE bl_runningjob
             SET job_id = :jobId, task_id = :taskId, submission = :submission, 
             state = :state, scheduler = :scheduler, service = :service, 
             sched_attr = :schedulerAttributes, scheduler_id = :schedulerId, 
             scheduler_parent_id = :schedulerParentId, 
             status_scheduler = :statusScheduler, status = :status, 
             status_reason = :statusReason, destination = :destination, 
             lb_timestamp = :lbTimestamp, submission_time = :submissionTime, 
             scheduled_at_site = :scheduledAtSite, start_time = :startTime, 
             stop_time = :stopTime, stageout_time = :stageOutTime, 
             getoutput_time = :getOutputTime, 
             output_request_time = :outputRequestTime,
             output_enqueue_time = :outputEnqueueTime, 
             getoutput_retry = :getOutputRetry, output_dir = :outputDirectory, 
             storage = :storage, 
             lfn = :lfn, application_return_code = :applicationReturnCode, 
             wrapper_return_code = :wrapperReturnCode, 
             process_status = :processStatus, closed = :closed 
             WHERE job_id = :jobId
                AND task_id = :taskId
                AND submission = :submission """

    def execute(self, binds, conn = None, transaction = False):
        """
        execute
        """
        
        objFormatter = RunningJobDBFormatter()
        
        ppBinds = objFormatter.preFormat(binds)
        
        self.dbi.processData(self.sql, ppBinds, conn = conn,
                             transaction = transaction)
        
        # try to catch error code?
        return
    
