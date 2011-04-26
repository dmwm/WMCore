#!/usr/bin/env python
"""
_Campaign.Delete_

Delete an operations campaign

"""



from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    _Delete_

    Delete an operations campaign

    """
    def execute(self, campaignName, conn = None, trans = False):
        """
        _execute_

        delete an operations campaign by name

        """
        self.sql = """
        DELETE from reqmgr_campaign where campaign_name='%s'
        """ % campaignName

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return

