#!/usr/bin/env python
"""
_Team.Delete_

Delete an operations team

"""
__revision__ = "$Id: Delete.py,v 1.1 2010/06/30 22:56:52 rpw Exp $"
__version__ = "$Revision: 1.1 $"

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
        DELETE from reqmgr_teams where team_name='%s'
        """ % teamName

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return
