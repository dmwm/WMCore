#!/usr/bin/env python
"""
_Save_

MySQL implementation of Jobs.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.1 2008/11/24 21:51:39 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.Save import Save as SaveJobMySQL

class Save(SaveJobMySQL):
    
    sql = """UPDATE wmbs_job SET JOBGROUP = :jobgroup, NAME = :name,
              LAST_UPDATE = :timestamp WHERE ID = :jobid"""
    
    def execute(self, jobid, jobgroup, name):
        binds = self.getBinds(jobgroup = jobgroup, name = name,
                              timestamp = self.timestamp(), jobid = jobid)

        self.dbi.processData(self.sql, binds)
