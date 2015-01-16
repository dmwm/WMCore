"""
_NewSubscription_

MySQL implementation of DBS3Buffer.NewSubscription

Created on May 2, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class NewSubscription(DBFormatter):
    """
    _NewSubscription_

    Create a new subscription in the database
    """

    sql = """INSERT IGNORE INTO dbsbuffer_dataset_subscription
            (dataset_id, site, custodial, auto_approve, move, priority, subscribed, delete_blocks)
            VALUES (:id, :site, :custodial, :auto_approve, :move, :priority, 0, :delete_blocks)
          """


    def execute(self, datasetID, subscriptionInfo,
                conn = None,
                transaction = False):
        """
        _execute_

        Execute the query and retrieve the subscriptions
        """
        binds = []

        # DeleteFromSource is not supported for move subscriptions
        delete_blocks = None
        if subscriptionInfo['CustodialSubType'] == 'Move':
                isMove = 1
        else:
                isMove = 0
                if subscriptionInfo.get('DeleteFromSource', False):
                    delete_blocks = 1

        for site in subscriptionInfo['CustodialSites']:
            subInfo = {'id' : datasetID,
                       'site' : site,
                       'custodial' : 1,
                       'auto_approve' : 1 if site in subscriptionInfo['AutoApproveSites'] else 0,
                       'move' : isMove,
                       'priority' : subscriptionInfo['Priority'],
                       'delete_blocks' : delete_blocks}
            binds.append(subInfo)

        for site in subscriptionInfo['NonCustodialSites']:
            subInfo = {'id' : datasetID,
                       'site' : site,
                       'custodial' : 0,
                       'auto_approve' : 1 if site in subscriptionInfo['AutoApproveSites'] else 0,
                       'move' : 0,
                       'priority' : subscriptionInfo['Priority'],
                       'delete_blocks' : delete_blocks}
            binds.append(subInfo)

        if not binds:
            return

        self.dbi.processData(self.sql,
                             binds = binds, conn = conn,
                             transaction = transaction)
