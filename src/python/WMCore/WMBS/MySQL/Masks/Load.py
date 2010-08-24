#!/usr/bin/env python
"""
_Load_

MySQL implementation of Masks.Load
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Load(DBFormatter):
    sql = """SELECT FirstEvent, LastEvent, FirstLumi, LastLumi, FirstRun,
             LastRun FROM wmbs_job_mask WHERE job = :jobid"""

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
    
    def execute(self, jobid, conn = None, transaction = False):
        binds = self.getBinds(jobid = jobid)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
