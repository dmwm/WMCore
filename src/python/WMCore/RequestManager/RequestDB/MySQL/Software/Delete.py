#!/usr/bin/env python
"""
_Delete_

Delete a software release from the database

"""



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
        self.sql += "software_name=:software_name"
        binds = {"software_name": softwareName}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return

