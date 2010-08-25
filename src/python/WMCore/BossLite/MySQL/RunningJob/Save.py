#!/usr/bin/env python
"""
_Save_

MySQL implementation of BossLite.Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.1 2010/03/30 10:22:58 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

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


    def execute(self, binds, conn = None, transaction = False):
        """
        This assumes that you are passing in binds in the same format
        as BossLite.DbObjects.RunningJob, and that you already have an id.  It was
        too long a function for me to want to write in Perugia while
        parsing the binds
        """
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
    
