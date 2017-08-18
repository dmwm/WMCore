#!/usr/bin/env python

"""
_ChangeState_

MySQL implementation of WorkUnit.ChangeState
Works on the the same jobs dictionary as Jobs.ChangeState
Expects a list of job objects with, at least, an id, a state (checked by the
wrapper class), a retry count for that state
"""

from __future__ import absolute_import, division, print_function

from WMCore.DataStructs.WorkUnit import JOB_WU_STATE_MAP, WU_STATES
from WMCore.Database.DBFormatter import DBFormatter


class ChangeState(DBFormatter):
    """
    _ChangeState_

    MySQL implementation of WorkUnit.ChangeState
    Works on the the same jobs dictionary as Jobs.ChangeState
    Expects a list of job objects with, at least, an id, a state (checked by the
    wrapper class), a retry count for that state
    """

    # Only increment retry_count if state changes and is one of the defined "newly submitted states"
    sql = ('UPDATE wmbs_workunit '
           'INNER JOIN wmbs_job_workunit_assoc ON wmbs_workunit.id = wmbs_job_workunit_assoc.workunit '
           'SET status = :status, '
           ' retry_count = '
           'CASE '
           ' WHEN (status != :status AND :status IN (%s)) THEN retry_count + 1 '
           ' ELSE retry_count '
           'END '
           'WHERE wmbs_job_workunit_assoc.job = :job'
           % ','.join([str(WU_STATES[state]) for state in ['running']])
           )

    def getBinds(self, jobs=None):
        """
        Return the workunit state from the job state and the job id
        """

        jobs = jobs or []

        def bindFunc(job):
            return {'job': job['id'], 'status': WU_STATES[JOB_WU_STATE_MAP[job['state']]]}

        return [bindFunc(job) for job in jobs]

    def execute(self, jobs=None, conn=None, transaction=False):
        """
        jobs is a list of Job objects (dicts)
        """

        jobs = jobs or []

        binds = self.getBinds(jobs)
        self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
        return
