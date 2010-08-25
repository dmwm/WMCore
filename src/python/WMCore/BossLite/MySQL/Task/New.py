#!/usr/bin/env python
"""
_Create_

MySQL implementation of BossLite.Jobs.Create
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2010/03/30 10:18:16 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
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
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
    
