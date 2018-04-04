#!/usr/bin/env python
"""
_SetLocationByLFN_

MySQL implementation of DBSBuffer.SetLocationByLFN
"""

from WMCore.Database.DBFormatter import DBFormatter


class SetLocationByLFN(DBFormatter):
    sql = """INSERT IGNORE INTO dbsbuffer_file_location (filename, location)
               SELECT df.id, dl.id
               FROM dbsbuffer_file df, dbsbuffer_location dl
               WHERE df.lfn = :lfn
               AND dl.pnn = :pnn
    """

    def execute(self, binds, conn=None, transaction=None):
        """
        Expect binds in the form {lfn, pnn}

        """
        self.dbi.processData(self.sql, binds, conn=conn,
                             transaction=transaction)
        return
