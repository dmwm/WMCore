#!/usr/bin/env python
"""
_MarkFinishedSubscriptions_

MySQL implementation of Subscriptions.MarkFinishedSubscriptions

Created on Aug 29, 2012

@author: dballest
"""

from time import time

from WMCore.Database.DBFormatter import DBFormatter

class MarkFinishedSubscriptions(DBFormatter):
    """
    Marks the given subscriptions as finished, and updates the timestamp
    """

    updateSQL = """UPDATE wmbs_subscription
                   SET finished = :finished, last_update = :timestamp
                   WHERE id = :id"""

    def execute(self, ids, finished = True, conn = None,
                transaction = False):
        """
        _execute_

        Update the subscriptions to match their finished status
        """

        if finished:
            finished = 1
        else:
            finished = 0

        #Make sure it's a list of IDs
        if not isinstance(ids, list):
            ids = [ids]

        binds = []
        for subId in ids:
            binds.append({'id': subId, 'finished': finished, 'timestamp': int(time())})

        if binds:
            self.dbi.processData(self.updateSQL, binds, conn = conn,
                                 transaction = transaction)

        return
