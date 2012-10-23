#!/usr/bin/env python
"""
_Campaign.ID_

Get the id for an operations campaign

"""



from WMCore.Database.DBFormatter import DBFormatter

class ID(DBFormatter):
    """
    _ID_

    retrieve the campaign_id from the campaign_name provided, return None
    if the campaign doesnt exist


    """
    def execute(self, campaignName, conn = None, trans = False):
        """
        _execute_

        get the campaign id for the campaignName provided else None

        """
        self.sql = """
        select campaign_id from reqmgr_campaign where campaign_name = :campaign_name
        """
        binds = {"campaign_name":campaignName}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        output = self.format(result)
        if len(output) == 0:
            return None
        return output[0][0]
