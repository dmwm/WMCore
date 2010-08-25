#!/usr/bin/env python
"""
_ListIncomplete_

MySQL implementation of Subscription.ListIncomplete
"""

__all__ = []
__revision__ = "$Id: ListIncomplete.py,v 1.1 2009/07/07 18:27:24 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListIncomplete(DBFormatter):
    sql = """SELECT id FROM wmbs_subscription
               INNER JOIN (SELECT fileset, COUNT(file) AS total_files
                           FROM wmbs_fileset_files GROUP BY fileset) wmbs_fileset_total_files
                 ON wmbs_subscription.fileset = wmbs_fileset_total_files.fileset
               LEFT OUTER JOIN (SELECT subscription, COUNT(file) AS complete_files
                           FROM wmbs_sub_files_complete GROUP BY subscription) wmbs_files_complete
                 ON wmbs_subscription.id = wmbs_files_complete.subscription
             WHERE (total_files != complete_files) OR
                   (complete_files IS Null AND total_files != 0)"""
    
    def format(self, result):
        results = DBFormatter.format(self, result)

        subIDs = []
        for row in results:
            subIDs.append(row[0])

        return subIDs
        
    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.format(result)
