#!/usr/bin/env python
"""
_SiblingSubscriptionsFailed_

Oracle implementation of Subscription.SiblingSubscriptionsFailed
"""

from WMCore.WMBS.MySQL.Subscriptions.SiblingSubscriptionsFailed import \
    SiblingSubscriptionsFailed as SiblingFailedMySQL

class SiblingSubscriptionsFailed(SiblingFailedMySQL):

    insert = """INSERT INTO wmbs_sub_files_complete (subscription, fileid)
                  SELECT :subscription, :fileid FROM dual
                  WHERE NOT EXISTS (SELECT wsfc.fileid FROM wmbs_sub_files_complete wsfc
                                    WHERE subscription = :subscription
                                    AND fileid = :fileid)"""
