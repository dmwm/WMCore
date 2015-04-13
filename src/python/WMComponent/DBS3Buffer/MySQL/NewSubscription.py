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
            (dataset_id, site, custodial, auto_approve, move, priority)
            VALUES (:id, :site, :custodial, :auto_approve, :move, :priority)
          """


    def execute(self, datasetID, subscriptionInfo,
                conn = None,
                transaction = False):
        """
        _execute_

        Execute the query and retrieve the subscriptions
        """
        binds = []
        for site in subscriptionInfo['CustodialSites']:
            subInfo = {'id' : datasetID,
                       'site' : site,
                       'custodial' : 1,
                       'auto_approve' : 1 if site in subscriptionInfo['AutoApproveSites'] and \
                                        not site.endswith('_MSS') else 0,
                       'move' : 1 if subscriptionInfo['CustodialSubType'] == 'Move' else 0,
                       'priority' : subscriptionInfo['Priority']}
            binds.append(subInfo)

        for site in subscriptionInfo['NonCustodialSites']:
            subInfo = {'id' : datasetID,
                       'site' : site,
                       'custodial' : 0,
                       'auto_approve' : 1 if site in subscriptionInfo['AutoApproveSites'] and \
                                        not site.endswith('_MSS') else 0,
                       'move' : 0,
                       'priority' : subscriptionInfo['Priority']}
            binds.append(subInfo)

        if not binds:
            return

        self.dbi.processData(self.sql,
                             binds = binds, conn = conn,
                             transaction = transaction)
