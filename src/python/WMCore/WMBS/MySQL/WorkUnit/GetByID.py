"""
_WorkUnit.Add_

MySQL implementation of WorkUnit.GetByID
"""

from __future__ import absolute_import, division, print_function

from WMCore.DataStructs.Run import Run
from WMCore.Database.DBFormatter import DBFormatter


class GetByID(DBFormatter):
    """
    _WorkUnit.GetByID_

    MySQL implementation of WorkUnit.GetByID
    """

    sql = ('SELECT wu.id as id, wu.taskid as taskid,  wu.retry_count as retry_count,'
           ' wu.last_unit_count as last_unit_count, wu.last_submit_time as last_submit_time, wu.status as status,'
           ' assoc.firstevent as firstevent, assoc.lastevent as lastevent,'
           ' assoc.fileid as fileid, assoc.run as run,  assoc.lumi as lumi'
           ' FROM wmbs_workunit wu'
           ' INNER JOIN wmbs_frl_workunit_assoc assoc'
           ' ON wu.id = assoc.workunit'
           ' WHERE wu.id = :wuid')

    intFields = ['id', 'taskid', 'retry_count', 'last_unit_count', 'last_submit_time', 'status',
                 'firstevent', 'lastevent', 'fileid']

    def formatOneDict(self, result):
        """
        _formatOneDict_

        Return the row as a dict
        """

        formattedResult = DBFormatter.formatDict(self, result)[0]
        return workUnitFormatter(formattedResult)

    def execute(self, wuid=None, conn=None, transaction=False):
        """
        Execute the lookup by ID
        """

        binds = {'wuid': wuid}
        result = self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
        return self.formatOneDict(result)


def workUnitFormatter(formattedResult):
    """
    Args:
        formattedResult: The one row that comes out of DBFormatter.formatDict

    Returns:
        A dictionary with integers correctly cast and the run/lumi as a Run object
    """

    intFields = ['id', 'taskid', 'retry_count', 'last_unit_count', 'last_submit_time', 'status',
                 'firstevent', 'lastevent', 'fileid']

    for field in intFields:
        if field in formattedResult:
            formattedResult[field] = int(formattedResult[field])

    # Run/lumi handled specially
    formattedResult['run_lumi'] = Run(int(formattedResult['run']), *[int(formattedResult['lumi'])])
    del formattedResult['run']
    del formattedResult['lumi']

    return formattedResult
