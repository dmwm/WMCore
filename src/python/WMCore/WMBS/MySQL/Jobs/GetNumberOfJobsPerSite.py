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

    def buildSQL(self, states, type):
        """
        _buildSQL_

        builds the sql statements; necessary for lists
        """


        baseSQL = """SELECT count(*) FROM wmbs_job
              WHERE location = (SELECT ID FROM wmbs_location WHERE site_name = :location)
              AND state IN (SELECT ID FROM wmbs_job_state js WHERE js.name IN (
        """

        typeSQL = """SELECT count(*) FROM wmbs_job
                       INNER JOIN wmbs_jobgroup ON wmbs_job.jobgroup = wmbs_jobgroup.id
                       INNER JOIN wmbs_subscription ON wmbs_jobgroup.subscription = wmbs_subscription.id
                       INNER JOIN wmbs_job_state ON wmbs_job.state = wmbs_job_state.id
                       INNER JOIN wmbs_location ON wmbs_job.location = wmbs_location.id
                       INNER JOIN wmbs_sub_types ON wmbs_subscription.subtype = wmbs_sub_types.id
                       WHERE wmbs_location.site_name = :location
                       AND wmbs_sub_types.name = :type
                       AND wmbs_job_state.name IN ("""

        if type:
            sql = typeSQL
        else:
            sql = baseSQL

        states = list(states)

        count = 0
        for state in states:
            if not count == 0:
                sql += ", "
            sql += ":state%i" %(count)
            count += 1
        sql += ")"

        return sql


    def format(self, results):
        """
        _format_

        """

        if len(results) == 0:
            return False
        else:
            return results[0].fetchall()[0]


    def buildBinds(self, location, states, type):
        """

        _buildBinds_

        Build a list of binds

        """

        binds = {}

        binds['location']     = location
        count = 0
        for state in states:
            binds["state%i" %(count)] = state
            count += 1

        if type:
            binds['type'] = type


        return binds


    def execute(self, location, states, type = None, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """

        sql = self.buildSQL(states, type)

        binds = self.buildBinds(location, states, type)

        #print "In Jobs.GetNumberOfJobsPerSite"
        #print sql
        #print binds

        result = self.dbi.processData(sql, binds, conn = conn, transaction = transaction)


        return self.format(result)
