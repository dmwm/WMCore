#!/usr/bin/env python
"""
_New_

MySQL implementation of Masks.New
"""

from WMCore.Database.DBFormatter import DBFormatter


class New(DBFormatter):
    sql = """INSERT INTO wmbs_job_mask (job, firstevent, lastevent, firstrun, lastrun, firstlumi, lastlumi, inclusivemask) VALUES
               (:jobid, :firstevent, :lastevent, :firstrun, :lastrun, :firstlumi, :lastlumi, :inclusivemask)"""

    def getDictBinds(self, jobList, inclusivemask):
        binds = []
        for job in jobList:
            binds.append({'jobid': job['id'], 'inclusivemask': inclusivemask,
                          'firstevent': job['mask']['FirstEvent'],
                          'lastevent': job['mask']['LastEvent'],
                          'firstrun': job['mask']['FirstRun'],
                          'lastrun': job['mask']['LastRun'],
                          'firstlumi': job['mask']['FirstLumi'],
                          'lastlumi': job['mask']['LastLumi'], })

        return binds

    def execute(self, jobList, inclusivemask=True, conn=None, transaction=False):
        binds = self.getDictBinds(jobList, inclusivemask)
        self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
        return
