#!/usr/bin/env python
"""
_SetLocation_

MySQL implementation of Files.SetLocation
"""

from WMCore.Database.DBFormatter import DBFormatter

from builtins import str, bytes

class SetLocation(DBFormatter):
    sql = """INSERT INTO wmbs_file_location (fileid, pnn)
                 SELECT :fileid, wpnn.id FROM wmbs_pnns wpnn
                 WHERE wpnn.pnn = :pnn"""

    def getBinds(self, file=None, pnn=None):
        if isinstance(pnn, (str, bytes)):
            return self.dbi.buildbinds(self.dbi.makelist(file), 'fileid',
                                       self.dbi.buildbinds(self.dbi.makelist(pnn), 'pnn'))
        elif isinstance(pnn, (list, set)):
            binds = []
            for l in pnn:
                binds.extend(self.dbi.buildbinds(self.dbi.makelist(file), 'fileid',
                                                 self.dbi.buildbinds(self.dbi.makelist(l), 'pnn')))
            return binds
        else:
            raise Exception("Type of pnn argument is not allowed: %s" \
                            % type(pnn))

    def execute(self, file, pnn, conn=None, transaction=None):
        binds = self.getBinds(file, pnn)

        result = self.dbi.processData(self.sql, binds, conn=conn,
                                      transaction=transaction)
        return
