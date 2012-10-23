#!/usr/bin/env python
"""
_Load_

MySQL implementation of Masks.Load
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Load(DBFormatter):
    sql = """SELECT DISTINCT FirstEvent, LastEvent, FirstLumi, LastLumi, FirstRun,
             LastRun FROM wmbs_job_mask WHERE job = :jobid"""

    def format(self, results):
        dictList = DBFormatter.formatDict(self, results)

        out = []

        for entry in dictList:
            tmpDict = {}
            tmpDict['FirstEvent'] = entry['firstevent']
            tmpDict['LastEvent']  = entry['lastevent']
            tmpDict['FirstLumi']  = entry['firstlumi']
            tmpDict['LastLumi']   = entry['lastlumi']
            tmpDict['FirstRun']   = entry['firstrun']
            tmpDict['LastRun']    = entry['lastrun']

            out.append(tmpDict)

        return out

    def execute(self, jobid, conn = None, transaction = False):
        binds = self.getBinds(jobid = jobid)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
