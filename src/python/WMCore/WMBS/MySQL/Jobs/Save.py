#!/usr/bin/env python
"""
_Save_

MySQL implementation of Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.5 2009/01/11 17:44:41 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class Save(DBFormatter):
    sql = """UPDATE wmbs_job SET JOBGROUP = :jobgroup, NAME = :name,
             LAST_UPDATE = unix_timestamp() WHERE ID = :jobid"""
    
    def execute(self, jobid, jobgroup, name, conn = None, transaction = False):
        binds = self.getBinds(jobgroup = jobgroup, name = name,
                              jobid = jobid)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
