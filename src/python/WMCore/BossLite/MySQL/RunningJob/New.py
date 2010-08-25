#!/usr/bin/env python
"""
_Create_

MySQL implementation of BossLite.Jobs.Create
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2010/03/30 10:23:30 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

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
    
