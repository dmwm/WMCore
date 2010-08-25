#!/usr/bin/env python
"""
_New_

MySQL implementation of BossLite.RunningJob.New
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.RunningJob import RunningJobDBFormatter

class New(DBFormatter):
    """
    BossLite.RunningJob.New
    """
    
    sql = """INSERT INTO bl_runningjob (job_id, task_id, submission, state, 
                scheduler, service, sched_attr, scheduler_id, 
                scheduler_parent_id, status_scheduler, status,
                status_reason, destination, lb_timestamp, submission_time,
                scheduled_at_site, start_time, stop_time, stageout_time, 
                getoutput_time, output_request_time, output_enqueue_time, 
                getoutput_retry, output_dir, storage, lfn, 
                application_return_code, wrapper_return_code, 
                process_status, closed)
             VALUES (:jobId, :taskId, :submission, :state, :scheduler, 
                     :service, :schedulerAttributes, :schedulerId, 
                     :schedulerParentId, :statusScheduler, :status, 
                     :statusReason, :destination, :lbTimestamp, 
                     :submissionTime, :scheduledAtSite, :startTime,
                     :stopTime, :stageOutTime, :getOutputTime, 
                     :outputRequestTime, :outputEnqueueTime, :getOutputRetry,
                     :outputDirectory, :storage, :lfn, :applicationReturnCode,
                     :wrapperReturnCode, :processStatus, :closed) """
    
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
    
