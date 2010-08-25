#!/usr/bin/env python
"""
_Load_

MySQL implementation of BossLite.RunningJob.Load
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2010/05/09 20:07:57 spigafi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.Common.System import strToList

class Load(DBFormatter):
    sql = """SELECT id AS id, job_id AS jobId, task_id AS taskId, submission AS submission,
                state AS state, scheduler AS scheduler, service AS service,
                sched_attr AS schedulerAttributes, scheduler_id AS schedulerId,
                scheduler_parent_id AS schedulerParentId, status_scheduler AS statusScheduler,
                status AS status, status_reason AS statusReason, destination AS destination,
                lb_timestamp AS lbTimestamp, submission_time AS submissionTime,
                scheduled_at_site AS scheduledAtSite, start_time AS startTime, stop_time AS stopTime,
                stageout_time AS stageOutTime, getoutput_time AS getOutputTime, 
                output_request_time AS outputRequestTime, output_enqueue_time AS outputEnqueueTime,
                getoutput_retry AS getOutputRetry, output_dir AS outputDirectory, storage AS storage,
                lfn AS lfn, application_return_code AS applicationReturnCode,
                wrapper_return_code AS wrapperReturnCode, process_status AS processStatus,
                closed AS closed
             FROM bl_runningjob
             WHERE %s"""


    def postFormat(self, res):
        """
        Format some crap
        """
        form = self.formatDict(res)
        final = []
        for entry in form:
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

    def execute(self, binds, conn = None, transaction = False):
        """
        This assumes that you are passing in binds in the same format
        as BossLite.DbObjects.RunningJob, and that you already have an id.  It was
        too long a function for me to want to write in Perugia while
        parsing the binds
        """
        
        whereStatement = []
        
        for x in binds:
            if type(binds[x]) == str :
                whereStatement.append( "%s = '%s'" % (x, binds[x]) )
            else:
                whereStatement.append( "%s = %s" % (x, binds[x]) )
                
        whereClause = ' AND '.join(whereStatement)

        sqlFilled = self.sql % (whereClause)
        
        result = self.dbi.processData(sqlFilled, {}, conn = conn,
                                      transaction = transaction)
        
        return self.postFormat(result)
    
