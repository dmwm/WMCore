#!/usr/bin/env python
"""
_Save_

Oracle implementation of Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.3 2009/01/11 17:49:37 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Jobs.Save import Save as SaveJobMySQL

class Save(SaveJobMySQL):
    sql = """UPDATE wmbs_job SET JOBGROUP = :jobgroup, NAME = :name,
             LAST_UPDATE = :timestamp WHERE ID = :jobid"""
    
    def execute(self, jobid, jobgroup, name, conn = None, transaction = False):
        binds = self.getBinds(jobgroup = jobgroup, name = name,
                              timestamp = self.timestamp(), jobid = jobid)

        self.dbi.processData(self.sql, binds, conn =  conn,
                             transaction = transaction)
