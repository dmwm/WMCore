#!/usr/bin/env python
"""
_Team.List_

Get a list of teams, including ID mappings


"""
__revision__ = "$Id: List.py,v 1.1 2010/06/30 22:57:10 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    _List_

    Get list of production teams from the DB

    """
    def execute(self, conn = None, trans = False):
        """
        _execute_

        Select list of teams from DB, return as map of team : ID

        """
        self.sql = "SELECT team_name, team_id FROM reqmgr_teams"


        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        return dict(result[0].fetchall())
