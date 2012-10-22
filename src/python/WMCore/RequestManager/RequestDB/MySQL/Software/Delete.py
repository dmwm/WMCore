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
    sql = """DELETE FROM reqmgr_software WHERE software_name = :name
                                           AND scram_arch = :arch
    """
    def execute(self, softwareName, scramArch = None, conn = None, trans = False):
        """
        _execute_

        Remove software name from database

        """
        binds = {"name": softwareName, 'arch': scramArch}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = trans)
        return
