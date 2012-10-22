#!/usr/bin/env python
"""
_Team.Delete_

Delete an operations team

"""



from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    _Delete_

    Delete an operations team

    """
    def execute(self, teamName, conn = None, trans = False):
        """
        _execute_

        delete an operations team by name

        """
        self.sql = """
        DELETE from reqmgr_teams where team_name=:team_name
        """
        binds = {"team_name": teamName}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
