#!/usr/bin/env python
"""
_Team.New_

Add a new production Team

"""

from WMCore.Database.DBFormatter import DBFormatter


class New(DBFormatter):
    """
    _New_

    Add a new production team in the ReqMgr DB

    """
    def execute(self, teamname, conn = None, trans = False):
        """
        _execute_

        Insert a new team with the name provided

        """
        self.sql = "INSERT INTO reqmgr_teams (team_name) VALUES (:team_name)"
        binds = {"team_name": teamname}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return self.format(result)
