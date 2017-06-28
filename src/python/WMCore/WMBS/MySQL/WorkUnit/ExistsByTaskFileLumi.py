#!/usr/bin/env python
"""
_WorkUnit.ExistsByTaskFileLumi_

MySQL implementation of WorkUnit.ExistsByTaskFileLumi
"""

from __future__ import absolute_import, division, print_function

from WMCore.Database.DBFormatter import DBFormatter


class ExistsByTaskFileLumi(DBFormatter):
    """
    _WorkUnit.ExistsByTaskFileLumi_

    MySQL implementation of WorkUnit.ExistsByTaskFileLumi
    """

    sql = ('SELECT wu.id as id FROM wmbs_workunit wu'
           ' INNER JOIN wmbs_frl_workunit_assoc assoc ON wu.id = assoc.workunit'
           ' WHERE wu.taskid=:taskid AND assoc.fileid=:fileid AND assoc.run=:run AND assoc.lumi=:lumi')

    def format(self, result):
        result = DBFormatter.format(self, result)
        if result:
            return result[0][0]
        else:
            return False

    def execute(self, taskid, fileid, run_lumi, conn=None, transaction=False):
        binds = {'taskid': taskid, 'fileid': fileid, 'run': run_lumi.run, 'lumi': run_lumi.lumis[0]}
        result = self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
        return self.format(result)
