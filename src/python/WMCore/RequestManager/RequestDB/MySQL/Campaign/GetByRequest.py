#!/usr/bin/env python
"""
_Campaign.GetByRequest_

Get the namefor an operations campaign given a request ID

"""



from WMCore.Database.DBFormatter import DBFormatter

class GetByRequest(DBFormatter):
    """
    _GetByRequest_

    retrieve the campaign_name for the request ID provided, return None
    if the campaign doesnt exist


    """
    def execute(self, requestId, conn = None, trans = False):
        """
        _execute_

        get the campaign id for the campaignName provided else None

        """
        self.sql = """
        SELECT campaign_name
         FROM reqmgr_campaign
          JOIN reqmgr_campaign_assoc
             ON reqmgr_campaign_assoc.campaign_id = reqmgr_campaign.campaign_id
               WHERE reqmgr_campaign_assoc.request_id=:request_id
        """
        binds = {"request_id":requestId}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        output = self.format(result)
        if len(output) == 0:
            return None
        return output[0][0]
