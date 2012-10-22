#!/usr/bin/env python
"""
_Campaign.NewAssoc_

Create a new association between a campaign and a request

"""





from WMCore.Database.DBFormatter import DBFormatter

class NewAssoc(DBFormatter):
    """
    _NewAssoc_

    Create a new association from a request to a production campaign

    """
    def execute(self, requestId, campaignId,
                conn = None, trans = False):
        """
        _execute_

        Make a new association between a request and a prod campaign

        """
        self.sql = "INSERT INTO reqmgr_campaign_assoc "
        self.sql += "(request_id, campaign_id ) "
        self.sql += "VALUES (:request_id, :campaign_id)"
        binds = {"request_id":requestId, "campaign_id":campaignId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
