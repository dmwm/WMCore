#!/usr/bin/env python
"""
_HeritageLFNParent_

MySQL implementation of DBSBufferFiles.HeritageLFNParent
"""




from WMCore.Database.DBFormatter import DBFormatter

class HeritageLFNParent(DBFormatter):
    sql = """INSERT INTO dbsbuffer_file_parent (child, parent)
               SELECT dfc.id, dfp.id FROM dbsbuffer_file dfc
               INNER JOIN dbsbuffer_file dfp
               WHERE dfc.lfn = :child
               AND dfp.lfn = :lfn
    """


    #sql = """INSERT INTO dbsbuffer_file_parent (child, parent)
    #           SELECT :child, dbsbuffer_file.id FROM dbsbuffer_file
    #             WHERE dbsbuffer_file.lfn = :lfn AND NOT EXISTS
    #               (SELECT child FROM dbsbuffer_file_parent
    #                WHERE child = :child AND
    #                      parent = (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn))"""

    def execute(self, parentLFNs, childLFN, conn = None, transaction = False):
        binds = []
        for parentLFN in parentLFNs:
            binds.append({"lfn": parentLFN, "child": childLFN})

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        result = self.dbi.processData("SELECT * FROM dbsbuffer_file_parent", None,
                                      conn = conn, transaction = transaction)

        return
