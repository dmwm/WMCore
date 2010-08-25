#!/usr/bin/env python
"""
_SelectJob_

MySQL implementation of BossLite.Job.SelectJob
"""

__all__ = []
__revision__ = "$Id: SelectJob.py,v 1.3 2010/05/10 12:57:39 spigafi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Job import JobDBFormatter

class SelectJob(DBFormatter):
    """
    BossLite.Job.SelectJob
    """
   
    sql = """SELECT id as id, 
                    job_id as jobId, 
                    task_id as taskId,
                    name as name, 
                    executable as executable, 
                    events as events,
                    arguments as arguments, 
                    stdin as StandardInput,
                    stdout as StandardOutput, 
                    stderr as StandardError,
                    input_files as inputFiles, 
                    output_files as outputFiles,
                    dls_destination as dlsDestination, 
                    submission_number as submissionNumber,
                    closed as closed
                FROM bl_job
                WHERE %s """

    def execute(self, binds, conn = None, transaction = False):
        """
        Load everything using the database ID
        """
        
        objFormatter = JobDBFormatter()
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
        
        ppResult = self.formatDict(result)
        return objFormatter.postFormat(ppResult)