#!/usr/bin/env python
"""
_Campaign.New_

Add a new production Campaign

"""

from WMCore.Database.DBFormatter import DBFormatter


class New(DBFormatter):
    """
    _New_

    Add a new production campaign in the ReqMgr DB

    """
    def execute(self, campaignname, conn = None, trans = False):
        """
        _execute_

        Insert a new campaign with the name provided

        """
        self.sql = "INSERT INTO reqmgr_campaign (campaign_name) VALUES (:campaign_name)"
        binds = {"campaign_name":campaignname}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.format(result)
