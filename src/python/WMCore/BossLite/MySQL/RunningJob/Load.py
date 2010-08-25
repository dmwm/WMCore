#!/usr/bin/env python
"""
_Load_

MySQL implementation of BossLite.RunningJob.Load
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.RunningJob import RunningJob
from WMCore.BossLite.DbObjects.RunningJob import RunningJobDBFormatter

class Load(DBFormatter):
    """
    BossLite.RunningJob.Load
    """
    
    sql = """SELECT id AS id, 
                    job_id AS jobId, 
                    task_id AS taskId, 
                    submission AS submission, 
                    state AS state, 
                    scheduler AS scheduler, 
                    service AS service, 
                    sched_attr AS schedulerAttributes, 
                    scheduler_id AS schedulerId, 
                    scheduler_parent_id AS schedulerParentId, 
                    status_scheduler AS statusScheduler, 
                    status AS status, 
                    status_reason AS statusReason, 
                    destination AS destination, 
                    lb_timestamp AS lbTimestamp, 
                    submission_time AS submissionTime, 
                    scheduled_at_site AS scheduledAtSite, 
                    start_time AS startTime, 
                    stop_time AS stopTime, 
                    stageout_time AS stageOutTime, 
                    getoutput_time AS getOutputTime, 
                    output_request_time AS outputRequestTime, 
                    output_enqueue_time AS outputEnqueueTime, 
                    getoutput_retry AS getOutputRetry, 
                    output_dir AS outputDirectory, 
                    storage AS storage,
                    lfn AS lfn, 
                    application_return_code AS applicationReturnCode, 
                    wrapper_return_code AS wrapperReturnCode, 
                    process_status AS processStatus, 
                    closed AS closed 
             FROM bl_runningjob
             WHERE %s """

    def execute(self, binds, conn = None, transaction = False):
        """
        execute
        """
        
        objFormatter = RunningJobDBFormatter()
        whereStatement = []
        
        for x in binds:
            if type(binds[x]) == str :
                whereStatement.append( "%s = '%s'" % (RunningJob.fields[x], binds[x]) )
            else:
                whereStatement.append( "%s = %s" % (RunningJob.fields[x], binds[x]) )
            
        whereClause = ' AND '.join(whereStatement)

        sqlFilled = self.sql % (whereClause)
        
        result = self.dbi.processData(sqlFilled, {}, conn = conn,
                                      transaction = transaction)
        
        ppResult = self.formatDict(result)
        return objFormatter.postFormat(ppResult)
    
