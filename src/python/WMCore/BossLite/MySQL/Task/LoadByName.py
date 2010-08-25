#!/usr/bin/env python
"""
_LoadByName_

MySQL implementation of BossLite.Task.LoadByName
"""

__all__ = []
__revision__ = "$Id: LoadByName.py,v 1.2 2010/05/10 12:54:43 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Task import TaskDBFormatter

class LoadByName(DBFormatter):
    """
    BossLite.Task.LoadByName
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
             WHERE name = :name """
    
    def execute(self, name, conn = None, transaction = False):
        """
        This requires a name, and can be done in bulk
        """

        objFormatter = TaskDBFormatter()
        
        if type(name) == list:
            binds = name
        else:
            binds = {'name': name}
        
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        
        return objFormatter.postFormat(result)
    
