#!/usr/bin/env python
"""
_Software.List_

List known software versions with ids

"""

__revision__ = "$Id: List.py,v 1.1 2010/07/01 19:15:51 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    _List_

    Get a map of software name to id from the DB

    """
    def execute(self, conn = None, trans = False):
        """
        _execute_

        Get mapping of software name to ids

        """
        self.sql = "SELECT software_name, software_id FROM reqmgr_software"

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        return dict(result[0].fetchall())
