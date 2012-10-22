#!/usr/bin/env python
"""
_GetNumberOfJobsPerSite_

MySQL implementation of Jobs.GetNumberOfJobsPerSite
"""

__all__ = []



import logging

from WMCore.Database.DBFormatter import DBFormatter

class GetNumberOfJobsPerSite(DBFormatter):
    """
    _GetLocation_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql = """SELECT count(*) FROM wmbs_job
           WHERE location = (SELECT ID FROM wmbs_location WHERE site_name = :location)
           AND jobgroup IN (SELECT ID FROM wmbs_jobgroup jg WHERE jg.subscription = :subscription)
           AND state = (SELECT ID FROM wmbs_job_state js WHERE js.name = :state)
           """

    def format(self, results):
        """
        _format_

        """

        if len(results) == 0:
            return False
        else:
            return results[0].fetchall()[0]


    def buildBinds(self, location, subscription, state):
        """

        _buildBinds_

        Build a list of binds

        """

        binds = {}

        binds['location']     = location
        binds['subscription'] = subscription
        binds['state' ]       = state

        return binds


    def execute(self, location, subscription, state, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """



        binds = self.buildBinds(location, subscription, state)

        result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)


        return self.format(result)
