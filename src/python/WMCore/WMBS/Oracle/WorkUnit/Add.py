"""
Oracle implementation of Add WorkUnit
"""
from __future__ import absolute_import, division, print_function

import time

from WMCore.WMBS.MySQL.WorkUnit.Add import Add as AddWorkUnitMySQL, MAX_EVENT


class Add(AddWorkUnitMySQL):
    """
    _Add_

    Use Oracle sequence instead of auto_increment and do it in a combined transaction
    """

    sql = """INSERT INTO wmbs_workunit (taskid, retry_count, last_unit_count, last_submit_time, status)
             VALUES (:taskid, :retry_count, :last_unit_count, :last_submit_time, :status)"""

    sql2 = """INSERT INTO wmbs_workunit (firstevent, lastevent, fileid, run, lumi)
             VALUES (:firstevent, :lastevent, :fileid, :run, :lumi)"""

    def execute(self, taskid=None, retry_count=0, last_unit_count=0, last_submit_time=time.time(),
                status=0, first_event=0, last_event=MAX_EVENT,
                fileid=None, run=None, lumi=None,
                conn=None, transaction=False):
        """
        Add the parts of the object that go into workunit and into the association table
        """

        if taskid:
            binds = {'taskid': taskid, 'retry_count': retry_count, 'last_unit_count': last_unit_count,
                     'last_submit_time': last_submit_time, 'status': status,
                     'firstevent': first_event, 'lastevent': last_event, 'fileid': fileid, 'run': run, 'lumi': lumi}

            self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
            self.dbi.processData(self.sql2, binds, conn=conn, transaction=transaction)
        else:
            raise RuntimeError("No way to create work units without task ID")

        return
