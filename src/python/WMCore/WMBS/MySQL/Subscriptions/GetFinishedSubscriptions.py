#!/usr/bin/env python
"""
_GetFinishedSubscriptions_

MySQL implementation of Subscription.GetFinishedSubscriptions

Created on Aug 30, 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetFinishedSubscriptions(DBFormatter):
    """
    Gets all the subscription marked as finished in the database
    """

    sql = """SELECT id
             FROM wmbs_subscription
             WHERE finished = 1
          """

    def execute(self, conn = None, transaction = False):
        """
        _execute_

        This DAO returns a list of dictionaries containing
        the key 'id' with the id of the finished subscriptions
        """

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
