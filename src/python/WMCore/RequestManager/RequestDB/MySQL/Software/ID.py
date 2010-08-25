#!/usr/bin/env python
"""
_Software.ID_

map the software name to the software id in the database

"""




from WMCore.Database.DBFormatter import DBFormatter

class ID(DBFormatter):
    """
    _ID_

    Get the database ID of the software name from the DB, or None, if not
    provided

    """
    def execute(self,softwareName, conn = None, trans = False):
        """
        _execute_

        get software name ID

        """
        self.sql = "SELECT software_id FROM reqmgr_software WHERE "
        self.sql += "software_name=\'%s\'" % softwareName

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)

        return self.formatOne(result)
