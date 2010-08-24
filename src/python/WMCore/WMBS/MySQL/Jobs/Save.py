#!/usr/bin/env python
"""
_Save_

MySQL implementation of Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.2 2008/11/21 17:06:48 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class Save(DBFormatter):
    sql = """UPDATE wmbs_job SET JOBGROUP = :jobgroup, NAME = :name,
              LAST_UPDATE = curtime() WHERE ID = :jobid"""
    
    def execute(self, jobid, jobgroup, name):
        binds = self.getBinds(jobgroup = jobgroup, name = name,
                              jobid = jobid)

        self.dbi.processData(self.sql, binds)
