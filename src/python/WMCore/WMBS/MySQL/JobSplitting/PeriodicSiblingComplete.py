"""
_PeriodicSiblingComplete_

MySQL implementation of JobSplitting.PeriodicSiblingComplete
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class PeriodicSiblingComplete(DBFormatter):
    """
    _PeriodicSiblingComplete_

    Check if all other subscriptions on the same input fileset
    as the specified subscription are complete (ie. have no
    available or acquiered files and the fileset is closed).
    Fileset closed is assumed for the query as the Harvest
    job splitter only does this check if it is.

    Returns either 0 (sibling subscription is not complete)
                or 1 (sibling subscription is complete).

    """

    sql = """SELECT CASE
                      WHEN COUNT(wmbs_sub_files_available.subscription) > 0 THEN 0
                      WHEN COUNT(wmbs_sub_files_acquired.subscription) > 0 THEN 0
                      ELSE 1
                    END
             FROM wmbs_subscription
             INNER JOIN wmbs_fileset ON
               wmbs_fileset.id = wmbs_subscription.fileset
             LEFT OUTER JOIN wmbs_subscription sibling_subscription ON
               sibling_subscription.fileset = wmbs_fileset.id AND
               sibling_subscription.id != wmbs_subscription.id
             LEFT OUTER JOIN wmbs_sub_files_available ON
               wmbs_sub_files_available.subscription = sibling_subscription.id
             LEFT OUTER JOIN wmbs_sub_files_acquired ON
               wmbs_sub_files_acquired.subscription = sibling_subscription.id
             WHERE wmbs_subscription.id = :subscription
             """

    def execute(self, subscription, conn = None, transaction = False):

        binds = { 'SUBSCRIPTION' : subscription }

        complete = self.dbi.processData(self.sql, binds, conn = conn,
                                        transaction = transaction)[0].fetchall()[0][0]

        return (complete == 1)
