#!/usr/bin/env python
"""
_Load_

MySQL implementation of BossLite.Job.Load
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2010/05/21 12:05:57 spigafi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Job import JobDBFormatter

class Load(DBFormatter):
    """
    BossLite.Job.Load
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