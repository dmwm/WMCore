"""
_ListSubscriptions_

MySQL implementation of DBS3Buffer.ListSubscriptions

Created on May 2, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class ListSubscriptions(DBFormatter):
    """
    _ListSubscriptions_

    List the associated subscriptions to a dataset given
    its dataset id
    """

    sql = """SELECT *
             FROM dbsbuffer_dataset_subscription
             WHERE dataset_id = :id
          """


    sqlWithSub = """SELECT *
                    FROM dbsbuffer_dataset_subscription
                    WHERE dataset_id = :id AND subscribed = :subscribed
                 """

    def execute(self, datasetID, subscribed = None,
                conn = None,
                transaction = False):
        """
        _execute_

        Execute the query and retrieve the subscriptions
        """
        if subscribed is not None:
            result = self.dbi.processData(self.sql,
                                          {'id' : datasetID,
                                           'subscribed' : subscribed}, conn = conn,
                                          transaction = transaction)
        else:
            result = self.dbi.processData(self.sql,
                                          {'id' : datasetID}, conn = conn,
                                          transaction = transaction)

        return self.formatDict(result)
