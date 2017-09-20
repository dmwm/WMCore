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

    idSQL = ('INSERT ALL'
             ' INTO wmbs_workunit (id, taskid, retry_count, last_unit_count, last_submit_time, status)'
             ' VALUES (wmbs_workunit_SEQ.nextval, :taskid, :retry_count, :last_unit_count, :last_submit_time, :status)'
             ' INTO wmbs_frl_workunit_assoc (workunit, firstevent, lastevent, fileid, run, lumi)'
             ' VALUES (wmbs_workunit_SEQ.nextval, :firstevent, :lastevent, :fileid, :run, :lumi)'
             ' SELECT * FROM DUAL')

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

            self.dbi.processData(self.idSQL, binds, conn=conn, transaction=transaction)
        else:
            raise RuntimeError("No way to create work units without task ID")

        return
