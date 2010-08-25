#!/usr/bin/env python
"""
_ListIncomplete_

Oracle implementation of Subscription.ListIncomplete
"""

__all__ = []
__revision__ = "$Id: ListIncomplete.py,v 1.1 2009/07/07 18:27:25 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.ListIncomplete import ListIncomplete as ListIncompleteMySQL

class ListIncomplete(ListIncompleteMySQL):
    sql = """SELECT id FROM wmbs_subscription
               INNER JOIN (SELECT fileset, COUNT(fileid) AS total_files
                           FROM wmbs_fileset_files GROUP BY fileset) wmbs_fileset_total_files
                 ON wmbs_subscription.fileset = wmbs_fileset_total_files.fileset
               LEFT OUTER JOIN (SELECT subscription, COUNT(fileid) AS complete_files
                           FROM wmbs_sub_files_complete GROUP BY subscription) wmbs_files_complete
                 ON wmbs_subscription.id = wmbs_files_complete.subscription
             WHERE (total_files != complete_files) OR
                   (complete_files IS Null AND total_files != 0)"""
