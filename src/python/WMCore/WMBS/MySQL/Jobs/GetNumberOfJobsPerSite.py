#!/usr/bin/env python
"""
_GetNumberOfJobsPerSite_

MySQL implementation of Jobs.GetNumberOfJobsPerSite
"""

__all__ = []
__revision__ = "$Id: GetNumberOfJobsPerSite.py,v 1.1 2009/09/10 15:41:07 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from WMCore.Database.DBFormatter import DBFormatter

class GetNumberOfJobsPerSite(DBFormatter):
    """
    _GetLocation_

    Retrieve all files that are associated with the given job from the
    database.
    """

    def buildSQL(self, states):
        """
        _buildSQL_

        builds the sql statements; necessary for lists
        """
        
        
        sql = """SELECT count(*) FROM wmbs_job
              WHERE location = (SELECT ID FROM wmbs_location WHERE site_name = :location)
              AND state IN (SELECT ID FROM wmbs_job_state js WHERE js.name IN (
        """

        states = list(states)
        
        count = 0
        for state in states:
            if not count == 0:
                sql += ", "
            sql += ":state%i" %(count)
            count += 1
        sql += "))"

        return sql
            

    def format(self, results):
        """
        _format_

        """

        if len(results) == 0:
            return False
        else:
            return results[0].fetchall()[0]


    def buildBinds(self, location, states):
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


        return binds

        
    def execute(self, location, states, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """

        sql = self.buildSQL(states)

        binds = self.buildBinds(location, states)

        #print "In Jobs.GetNumberOfJobsPerSite"
        #print sql
        #print binds

        result = self.dbi.processData(sql, binds, conn = conn, transaction = transaction)


        return self.format(result)
