#!/usr/bin/env python
"""
_SelectTask_

MySQL implementation of BossLite.Task.SelectTask
"""

__all__ = []
__revision__ = "$Id: SelectTask.py,v 1.2 2010/05/09 20:01:51 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class SelectTask(DBFormatter):
    sql = """SELECT id as id, name as name, dataset as dataset, start_dir as startDirectory, output_dir as outputDirectory,
                global_sandbox as globalSandbox, cfg_name as cfgName, server_name as serverName, job_type as jobType,
                user_proxy as user_proxy, outfile_basename as outfileBasename, common_requirements as commonRequirements
             FROM bl_task
             WHERE %s
                """

    def postFormat(self, res):
        """
        Format in human readable, bulk compatible form
        """
        form = self.formatDict(res)
        final = []
        for entry in form:
            result = {}
            result['id']                 = entry['id']
            result['startDirectory']     = entry['startdirectory']
            result['outputDirectory']    = entry['outputdirectory']
            result['globalSandbox']      = entry['globalsandbox']
            result['cfgName']            = entry['cfgname']
            result['serverName']         = entry['servername']
            result['jobType']            = entry['jobtype']
            result['outfileBasename']    = entry['outfilebasename']
            result['commonRequirements'] = entry['commonrequirements']
            result['name']               = entry['name']
            result['dataset']            = entry['dataset']
            result['user_proxy']         = entry['user_proxy']
            final.append(result)

        return final

    def execute(self, binds, conn = None, transaction = False):
        """
        Load a task, or a list of tasks, as a function of a column 'column' with
        value 'value'
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
