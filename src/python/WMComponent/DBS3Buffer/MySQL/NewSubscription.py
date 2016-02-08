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
            (dataset_id, site, custodial, auto_approve, move, priority, subscribed, phedex_group, delete_blocks)
            VALUES (:id, :site, :custodial, :auto_approve, :move, :priority, 0, :phedex_group, :delete_blocks)
          """

    def _createPhEDExSubBinds(self, datasetID, subscriptionInfo, custodialFlag):

        # DeleteFromSource is not supported for move subscriptions
        delete_blocks = None
        if custodialFlag:
            sites = subscriptionInfo['CustodialSites']
            phedex_group = subscriptionInfo['CustodialGroup']
            isMove = 1 if subscriptionInfo['CustodialSubType'] == 'Move' else 0
        else:
            sites = subscriptionInfo['NonCustodialSites']
            phedex_group = subscriptionInfo['NonCustodialGroup']
            isMove = 1 if subscriptionInfo['NonCustodialSubType'] == 'Move' else 0
        if not isMove:
            delete_blocks = 1 if subscriptionInfo.get('DeleteFromSource', False) else None

        binds = []
        for site in sites:
            binds.append({'id': datasetID,
                          'site': site,
                          'custodial': custodialFlag,
                          'auto_approve': 1 if site in subscriptionInfo['AutoApproveSites'] else 0,
                          'move': isMove,
                          'priority': subscriptionInfo['Priority'],
                          'phedex_group': phedex_group,
                          'delete_blocks': delete_blocks})
        return binds

    def execute(self, datasetID, subscriptionInfo, conn=None, transaction=False):
        """
        _execute_

        Execute the query and retrieve the subscriptions
        """
        binds = self._createPhEDExSubBinds(datasetID, subscriptionInfo, True)
        binds.extend(self._createPhEDExSubBinds(datasetID, subscriptionInfo, False))

        if not binds:
            return

        self.dbi.processData(self.sql, binds=binds, conn=conn, transaction=transaction)
