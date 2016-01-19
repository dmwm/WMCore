#!/usr/bin/env python
"""
_GetUnsubscribedDatasets_

MySQL implementation of PhEDExInjector.Database.GetUnsubscribedDatasets
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetUnsubscribedDatasets(DBFormatter):
    """
    _GetUnsubscribedDatasets_

    Gets the unsubscribed datasets from DBSBuffer
    """

    sql = """SELECT DISTINCT dbsbuffer_dataset_subscription.id,
                             dbsbuffer_dataset.path,
                             dbsbuffer_dataset_subscription.site,
                             dbsbuffer_dataset_subscription.custodial,
                             dbsbuffer_dataset_subscription.auto_approve,
                             dbsbuffer_dataset_subscription.move,
                             dbsbuffer_dataset_subscription.priority,
                             dbsbuffer_dataset_subscription.phedex_group
               FROM dbsbuffer_dataset_subscription
               INNER JOIN dbsbuffer_dataset ON
                   dbsbuffer_dataset.id = dbsbuffer_dataset_subscription.dataset_id
               INNER JOIN dbsbuffer_algo_dataset_assoc ON
                 dbsbuffer_dataset.id = dbsbuffer_algo_dataset_assoc.dataset_id
               INNER JOIN dbsbuffer_file ON
                 dbsbuffer_algo_dataset_assoc.id = dbsbuffer_file.dataset_algo
             WHERE dbsbuffer_dataset_subscription.subscribed = 0 AND
                   (dbsbuffer_file.status = 'GLOBAL' OR dbsbuffer_file.status = 'InDBS') AND
                   dbsbuffer_file.in_phedex = 1 AND
                   dbsbuffer_dataset.path != 'bogus'"""

    def formatToPhEDEx(self, result):
        """
        _formatToPhEDEx_

        Format the result to the same format
        as the PhEDEx datasvc uses
        """
        for entry in result:
            if entry['auto_approve'] == 1:
                entry['request_only'] = 'n'
            else:
                entry['request_only'] = 'y'
            del entry['auto_approve']
            for key in ['custodial', 'move']:
                if entry[key] == 1:
                    entry[key] = 'y'
                else:
                    entry[key] = 'n'
            entry['priority'] = entry['priority'].lower()
        return result

    def execute(self, conn = None, transaction = False):

        result = self.dbi.processData(self.sql, conn = conn,
                                      transaction = transaction)
        return self.formatToPhEDEx(self.formatDict(result))
