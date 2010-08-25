#!/usr/bin/env python
"""
_SetLocationByLFN_

MySQL implementation of DBSBuffer.SetLocationByLFN
"""

__revision__ = "$Id: SetLocationByLFN.py,v 1.1 2010/03/09 18:32:22 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetLocationByLFN(DBFormatter):
    sql = """INSERT INTO dbsbuffer_file_location (filename, location)
               SELECT df.id, dl.id
               FROM dbsbuffer_file df
               INNER JOIN dbsbuffer_location dl
               WHERE df.lfn = :lfn
               AND dl.se_name = :sename
    """

    
    def execute(self, binds, conn = None, transaction = None):
        """
        Expect binds in the form {lfn, sename}

        """
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
