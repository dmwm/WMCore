#!/usr/bin/env python
"""
_Create_

MySQL implementation of BossLite.Task.LoadByID
"""

__all__ = []
__revision__ = "$Id: LoadByID.py,v 1.1 2010/03/30 10:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadByID(DBFormatter):
    sql = """SELECT name as name, dataset as dataset, start_dir as startDirectory, output_dir as outputDirectory,
                global_sandbox as globalSandbox, cfg_name as cfgName, server_name as serverName, job_type as jobType,
                user_proxy as user_proxy, outfile_basename as outfileBasename, common_requirements as commonRequirements
             FROM bl_task
             WHERE
                id = :id
                """

    def format(self, res):
        """
        Format in human readable, bulk compatible form
        """
        form = self.formatDict(res)
        final = []
        for entry in form:
            result = {}
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



    def execute(self, id, conn = None, transaction = False):
        """
        This requires an ID, and can be done in bulk
        """

        if type(id) == list:
            binds = id
        else:
            binds = {'id': id}
        
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
    
