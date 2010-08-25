#!/usr/bin/env python
"""
_Save_

MySQL implementation of BossLite.Task.Save
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Task import TaskDBFormatter

class Save(DBFormatter):
    """
    BossLite.Task.Save
    """
    
    sql = """UPDATE bl_task
             SET dataset = :dataset, 
                 start_dir = :startDirectory, 
                 output_dir = :outputDirectory,
                 global_sandbox = :globalSandbox, 
                 cfg_name = :cfgName, 
                 server_name = :serverName, 
                 job_type = :jobType, 
                 total_events = :totalEvents,
                 user_proxy = :user_proxy, 
                 outfile_basename = :outfileBasename, 
                 common_requirements = :commonRequirements
             WHERE name = :name """

    def execute(self, binds, conn = None, transaction = False):
        """
        This assumes that you are passing in binds in the same format
        as BossLite.DbObjects.Task.
        """
        
        objFormatter = TaskDBFormatter()
        
        ppBinds = objFormatter.preFormat(binds)

        self.dbi.processData(self.sql, ppBinds, conn = conn,
                             transaction = transaction)
        
        # try to catch error code?
        return
    