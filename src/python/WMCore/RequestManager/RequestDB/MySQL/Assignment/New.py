#!/usr/bin/env python
"""
_Assignment.New_

Create a new assignment between a team and a request

"""


__revision__ = "$Id: New.py,v 1.1 2010/07/01 19:03:09 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    _New_

    Create a new assignment from a request to a production team

    """
    def execute(self, requestId, teamId, priorityMod,
                conn = None, trans = False):
        """
        _execute_

        Make a new association between a request and a prod team

        """
        self.sql = "INSERT INTO reqmgr_assignment "
        self.sql += "(request_id, team_id, priority_modifier ) "
        self.sql += "VALUES (%s, %s, %s)" % ( requestId, teamId, priorityMod)

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return

