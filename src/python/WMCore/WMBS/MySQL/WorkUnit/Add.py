#!/usr/bin/env python
"""
_WorkUnit.Add_

MySQL implementation of WorkUnit.Add
"""

from __future__ import absolute_import, division, print_function

from WMCore.Database.DBFormatter import DBFormatter


class Add(DBFormatter):
    """
    _WorkUnit.Add_

    MySQL implementation of WorkUnit.Add
    """

    wuSQL = ('INSERT INTO wmbs_workunit (taskid, retry_count, last_unit_count, last_submit_time, status)'
             ' VALUES (:taskid, :retry_count, :last_unit_count, :last_submit_time, :status)')

    assocSQL = ('INSERT INTO wmbs_frl_workunit_assoc (workunit, firstevent, lastevent, fileid, run, lumi)'
                ' VALUES (LAST_INSERT_ID(), :firstevent, :lastevent, :fileid, :run, :lumi)')

    def execute(self, taskid=None, retry_count=0, last_unit_count=0, last_submit_time=0,
                status=0, first_event=0, last_event=0,
                fileid=None, run=None, lumi=None,
                conn=None, transaction=False):
        """
        Add the parts of the object that go into workunit and into the association table
        """

        # Add the part for the work unit table
        if taskid:
            binds = {'taskid': taskid, 'retry_count': retry_count, 'last_unit_count': last_unit_count,
                     'last_submit_time': last_submit_time, 'status': status}
            self.dbi.processData(self.wuSQL, binds, conn=conn, transaction=transaction)
        else:
            raise RuntimeError("No way to create work units without task ID or name")

        # And add the association information using the primary key from the first insertion
        binds = {'firstevent': first_event, 'lastevent': last_event, 'fileid': fileid, 'run': run, 'lumi': lumi}
        self.dbi.processData(self.assocSQL, binds, conn=conn, transaction=transaction)

        return
