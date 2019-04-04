#!/usr/bin/env python
"""
_New_

Oracle implementation of Masks.New
"""

from WMCore.WMBS.MySQL.Masks.New import New as NewMasksMySQL


class New(NewMasksMySQL):
    sql = NewMasksMySQL.sql

    def getDictBinds(self, jobList, inclusivemask):
        binds = []
        maskV = 'T' if inclusivemask else 'F'
        for job in jobList:
            binds.append({'jobid': job['id'], 'inclusivemask': maskV,
                          'firstevent': job['mask']['FirstEvent'],
                          'lastevent': job['mask']['LastEvent'],
                          'firstrun': job['mask']['FirstRun'],
                          'lastrun': job['mask']['LastRun'],
                          'firstlumi': job['mask']['FirstLumi'],
                          'lastlumi': job['mask']['LastLumi']})

        return binds

    def execute(self, jobList, inclusivemask=True, conn=None, transaction=False):
        binds = self.getDictBinds(jobList, inclusivemask)
        self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
        return
