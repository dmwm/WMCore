#!/usr/bin/env python
"""
_HeritageLFNParent_

MySQL implementation of DBSBufferFiles.HeritageLFNParent
"""

__revision__ = "$Id: HeritageLFNParent.py,v 1.2 2009/12/17 21:56:42 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class HeritageLFNParent(DBFormatter):
    sql = """INSERT INTO dbsbuffer_file_parent (child, parent)
               SELECT :child, dbsbuffer_file.id FROM dbsbuffer_file
                 WHERE dbsbuffer_file.lfn = :lfn AND NOT EXISTS
                   (SELECT child FROM dbsbuffer_file_parent
                    WHERE child = :child AND
                          parent = (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn))"""
    
    def execute(self, parentLFNs, childID, conn = None, transaction = False):
        binds = []
        for parentLFN in parentLFNs:
            binds.append({"lfn": parentLFN, "child": childID})

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        result = self.dbi.processData("SELECT * FROM dbsbuffer_file_parent", None,
                                      conn = conn, transaction = transaction)
        return
