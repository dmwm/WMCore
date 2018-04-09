#!/usr/bin/env python
"""
_SetLocationByLFN_

MySQL implementation of DBSBuffer.SetLocationByLFN
"""


import logging
from Utils.IteratorTools import grouper
from WMCore.Database.DBFormatter import DBFormatter

class SetLocationByLFN(DBFormatter):
    sql = """INSERT IGNORE INTO dbsbuffer_file_location (filename, location)
               SELECT df.id, dl.id
               FROM dbsbuffer_file df, dbsbuffer_location dl
               WHERE df.lfn = :lfn
               AND dl.pnn = :pnn
    """


    def execute(self, binds, conn = None, transaction = None):
        """
        Expect binds in the form {lfn, pnn}

        """
        count = 0
        for sliceBinds in grouper(binds, 10000):
            self.dbi.processData(self.sql, sliceBinds, conn = conn,
                                 transaction = transaction)
            count += len(sliceBinds)
            logging.info("Inserted %d binds out of %d", count, len(binds))
        return
