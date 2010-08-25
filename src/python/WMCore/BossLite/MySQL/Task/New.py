#!/usr/bin/env python
"""
_New_

MySQL implementation of BossLite.Task.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2010/05/10 12:54:43 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.DbObjects.Task import TaskDBFormatter

class New(DBFormatter):
    """
    Task.New
    """
    
    sql = """INSERT INTO bl_task (name, dataset, start_dir, output_dir,
                global_sandbox, cfg_name, server_name, job_type,
                user_proxy, outfile_basename, common_requirements)
             VALUES
                (:name, :dataset, :startDirectory,
                :outputDirectory, :globalSandbox,
                :cfgName, :serverName, :jobType, :user_proxy,
                :outfileBasename, :commonRequirements)
                """

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
    
