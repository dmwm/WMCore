#!/usr/bin/env python
"""
_GetAllJobGroups_

MySQL implementation of Subscriptions.GetJobGroups
"""

__revision__ = "$Id: GetAllJobGroups.py,v 1.1 2010/01/14 16:16:39 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetAllJobGroups(DBFormatter):
    sql = """SELECT DISTINCT wmbs_jobgroup.id FROM wmbs_jobgroup
               WHERE wmbs_jobgroup.subscription = :subscription"""

    def format(self, results):
        """
        _format_

        Format the results into a single list of job group IDs.
        """
        results = DBFormatter.format(self, results)

        jobGroupIDs = []
        for result in results:
            for row in result:
                jobGroupIDs.append(int(row))

        return jobGroupIDs
        
    def execute(self, subscription = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"subscription": subscription},
                                      conn = conn, transaction = transaction)
        return self.format(result)
