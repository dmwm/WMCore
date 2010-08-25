#!/usr/bin/env python
"""
_SelectTask_

MySQL implementation of BossLite.Task.SelectTask
"""

__all__ = []
__revision__ = "$Id: SelectTask.py,v 1.3 2010/05/10 12:54:43 spigafi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Task import TaskDBFormatter

class SelectTask(DBFormatter):
    """
    BossLite.Task.SelectTask
    """
    
    sql = """SELECT id as id, 
                    name as name, 
                    dataset as dataset, 
                    start_dir as startDirectory, 
                    output_dir as outputDirectory, 
                    global_sandbox as globalSandbox, 
                    cfg_name as cfgName, 
                    server_name as serverName, 
                    job_type as jobType, 
                    user_proxy as user_proxy, 
                    outfile_basename as outfileBasename, 
                    common_requirements as commonRequirements
             FROM bl_task
             WHERE %s """

    def execute(self, binds, conn = None, transaction = False):
        """
        Load a task, or a list of tasks, as a function of a column 'column' with
        value 'value'
        """
        
        objFormatter = TaskDBFormatter()
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
