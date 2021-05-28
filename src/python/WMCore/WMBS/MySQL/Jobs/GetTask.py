#!/usr/bin/env python
"""
_GetTask_

MySQL implementation of Jobs.Task
"""

import logging

from WMCore.Database.DBFormatter import DBFormatter

from builtins import int

__all__ = []


class GetTask(DBFormatter):
    sql = """SELECT wmwf.task AS task, wmj.id AS jobid FROM wmbs_workflow wmwf
               INNER JOIN wmbs_subscription wms ON wmwf.id = wms.workflow
               INNER JOIN wmbs_jobgroup wmjg ON wms.id = wmjg.subscription
               INNER JOIN wmbs_job wmj ON wmjg.id = wmj.jobgroup
               WHERE wmj.id = :jobid
               """

    def format(self, results):
        """
        Should create one dictionary of the form {'jobid':'taskName'}

        """
        dictionary = self.formatDict(results)

        final = {}

        for entry in dictionary:
            final[entry['jobid']] = entry['task']

        return final

    def execute(self, jobID, conn=None, transaction=False):
        """
        Should handle bulk and regular attempts to find the task
        """

        if isinstance(jobID, list):
            if len(jobID) == 0:
                return {}
            binds = []
            for ID in jobID:
                binds.append({'jobid': int(ID)})
        elif isinstance(jobID, int):
            binds = {'jobid': int(jobID)}
        else:
            logging.error('Incompatible jobid in GetTask')
            return

        results = self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)

        return self.format(results)
