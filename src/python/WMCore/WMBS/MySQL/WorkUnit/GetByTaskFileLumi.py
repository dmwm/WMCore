"""
MySQL implementation of WorkUnit.GetByTaskFileLumi which uses the same formatter as GetByID
"""
from __future__ import absolute_import, division, print_function

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.MySQL.WorkUnit.GetByID import workUnitFormatter


class GetByTaskFileLumi(DBFormatter):
    """
    MySQL implementation of WorkUnit.GetByTaskFileLumi which uses the workUnitFormatter from GetByID
    """

    sql = ('SELECT wu.id as id, wu.taskid as taskid, wu.retry_count as retry_count,'
           ' wu.last_unit_count as last_unit_count, wu.last_submit_time as last_submit_time, wu.status as status,'
           ' assoc.firstevent as firstevent, assoc.lastevent as lastevent,'
           ' assoc.fileid as fileid, assoc.run as run,  assoc.lumi as lumi'
           ' FROM wmbs_workunit wu'
           ' INNER JOIN wmbs_frl_workunit_assoc assoc ON wu.id = assoc.workunit'
           ' WHERE wu.taskid=:taskid AND assoc.fileid=:fileid AND assoc.run=:run AND assoc.lumi=:lumi')

    def execute(self, taskid=None, fileid=None, run_lumi=None, conn=None, transaction=False):
        """
        Execute the lookup by task, file, run, and lumi
        """

        binds = {'taskid': taskid, 'fileid': fileid, 'run': run_lumi.run, 'lumi': run_lumi.lumis[0]}
        result = self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
        return self.formatOneDict(result)

    def formatOneDict(self, result):
        """
        _formatOneDict_

        Return the row as a dict
        """

        formattedResult = DBFormatter.formatDict(self, result)[0]
        return workUnitFormatter(formattedResult)
