#!/usr/bin/env python
"""
_Campaign.List_

Get a list of campaigns, including ID mappings


"""



from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    _List_

    Get list of production campaigns from the DB

    """
    def execute(self, conn = None, trans = False):
        """
        _execute_

        Select list of campaigns from DB, return as map of campaign : ID

        """
        self.sql = "SELECT campaign_name, campaign_id FROM reqmgr_campaign"


        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        return dict(result[0].fetchall())
