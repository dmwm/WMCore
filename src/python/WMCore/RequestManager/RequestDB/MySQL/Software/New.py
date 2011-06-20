#!/usr/bin/env python
"""
_Software.New_

Record a new Software version available for requests

"""




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
        self.sql += "VALUES (:software_name)"
        binds = {"software_name": softwareName}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return




