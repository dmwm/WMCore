#!/usr/bin/env python
"""
_Load_

MySQL implementation of Masks.Load
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2008/11/20 17:20:48 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Load(DBFormatter):
    sql = """select FirstEvent, LastEvent, FirstLumi, LastLumi, FirstRun,
             LastRun from wmbs_job_mask where job = :jobid"""

    def format(self, results):
        results = DBFormatter.format(self, results)

        out = {}
        out["FirstEvent"] = results[0][0]
        out["LastEvent"] = results[0][1]
        out["FirstLumi"] = results[0][2]
        out["LastLumi"] = results[0][3]
        out["FirstRun"] = results[0][4]
        out["LastRun"] = results[0][5]

        return out
    
    def execute(self, jobid):
        binds = self.getBinds(jobid = jobid)
        result = self.dbi.processData(self.sql, binds)
        return self.format(result)
