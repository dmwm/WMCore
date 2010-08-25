#!/usr/bin/env python
"""
_LoadByID_

MySQL implementation of BossLite.Task.LoadByID
"""

__all__ = []
__revision__ = "$Id: LoadByID.py,v 1.2 2010/05/10 12:54:43 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Task import TaskDBFormatter

class LoadByID(DBFormatter):
    """
    BossLite.Task.LoadByID
    """
    
    sql = """SELECT name as name, 
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
             WHERE
                id = :id
                """
    
    def execute(self, tmpId, conn = None, transaction = False):
        """
        This requires an ID, and can be done in bulk
        """

        objFormatter = TaskDBFormatter()
        
        if type(tmpId) == list:
            binds = tmpId
        else:
            binds = {'id': tmpId}
        
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        
        
        return objFormatter.postFormat(result)
    