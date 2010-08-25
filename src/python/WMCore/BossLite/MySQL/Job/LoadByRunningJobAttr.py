#!/usr/bin/env python
"""
_LoadByRunningJobAttr_

MySQL implementation of BossLite.Jobs.LoadByRunningJobAttr
"""

__all__ = []
__revision__ = "$Id: LoadByRunningJobAttr.py,v 1.3 2010/05/19 13:25:28 spigafi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Job import JobDBFormatter

class LoadByRunningJobAttr(DBFormatter):
    """
    BossLite.Jobs.LoadByRunningJobAttr
    """
    
    sql = """SELECT bl_job.id as id,
                bl_job.job_id as jobId, 
                bl_job.task_id as taskId,
                bl_job.name as name, 
                bl_job.executable as executable, 
                bl_job.events as events,
                bl_job.arguments as arguments, 
                bl_job.stdin as StandardInput,
                bl_job.stdout as StandardOutput, 
                bl_job.stderr as StandardError,
                bl_job.input_files as inputFiles, 
                bl_job.output_files as outputFiles,
                bl_job.dls_destination as dlsDestination,
                bl_job.submission_number as submissionNumber,
                bl_job.closed as closed
                FROM bl_job
                INNER JOIN bl_runningjob ON 
                        bl_runningjob.job_id = bl_job.job_id
                WHERE bl_runningjob.submission = 
                        ( SELECT MAX(submission) FROM bl_runningjob 
                            WHERE bl_runningjob.job_id = bl_job.job_id )
                        AND %s """

    def execute(self, binds, limit = None, conn = None, transaction = False):
        """
        Load a job based on the attributes of the most recent runningJob
        """
        
        objFormatter = JobDBFormatter()
        whereStatement = []
        
        for x in binds:
            if type(binds[x]) == str :
                whereStatement.append( "bl_runningjob.%s = '%s'" % (x, binds[x]) )
            else:
                whereStatement.append( "bl_runningjob.%s = %s" % (x, binds[x]) )
                
        whereClause = ' AND '.join(whereStatement)

        sqlFilled = self.sql % (whereClause)
        
        if limit :
            if type(limit) == list and len(limit) == 2 :
                sqlFilled += """ AND bl_job.id > %s LIMIT %s """ (limit[0], limit[1]) 
                # sqlFilled += """ LIMIT %s, %s """ % (limit[0], limit[1])
            # elif type(limit) == list and len(limit) == 1 :
            #      sqlFilled += """ LIMIT %s """ % (limit[0])
            # elif type(limit) == int and limit >= 0 :
            #     sqlFilled += """ LIMIT %s """ % (limit)
            # if something is wrong, the LIMIT is ignored
        
        result = self.dbi.processData(sqlFilled, {}, conn = conn,
                                      transaction = transaction)
        
        ppResult = self.formatDict(result)
        return objFormatter.postFormat(ppResult)
