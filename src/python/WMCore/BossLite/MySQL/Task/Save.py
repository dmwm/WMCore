#!/usr/bin/env python
"""
_Create_

MySQL implementation of BossLite.Jobs.Create
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.1 2010/03/30 10:19:33 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Save(DBFormatter):
    sql = """UPDATE bl_task
             SET dataset = :dataset, start_dir = :startDirectory, output_dir = :outputDirectory,
                global_sandbox = :globalSandbox, cfg_name = :cfgName, server_name = :serverName, job_type = :jobType,
                user_proxy = :user_proxy, outfile_basename = :outfileBasename, common_requirements = :commonRequirements
             WHERE
                name = :name
                """


    def execute(self, binds, conn = None, transaction = False):
        """
        This assumes that you are passing in binds in the same format
        as BossLite.DbObjects.Task.
        """
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
    
