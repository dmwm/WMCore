#!/usr/bin/env python
"""
_Delete_

Delete a software release from the database

"""
__revision__ = "$Id: Delete.py,v 1.1 2010/07/01 19:15:51 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    """
    _Delete_

    Delete a software version from the DB

    """
    def execute(self, softwareName, conn = None, trans = False):
        """
        _execute_

        Remove software name from database

        """
        self.sql = "delete from reqmgr_software where "
        self.sql += "software_name=\'%s\'" % softwareName

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return

