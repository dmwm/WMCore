#!/usr/bin/env python
"""
_Software.New_

Record a new Software version available for requests

"""

__revision__ = "$Id: New.py,v 1.1 2010/07/01 19:15:51 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    _New_

    Insert a new software version into the DB

    """
    def execute(self, softwareName, conn = None, trans = False):
        """
        _execute_

        Add the named SW version to the DB

        """
        self.sql = "INSERT INTO reqmgr_software (software_name) "
        self.sql += "VALUES (\'%s\')" % softwareName

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
        return




