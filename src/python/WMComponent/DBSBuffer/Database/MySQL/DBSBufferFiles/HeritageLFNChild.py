#!/usr/bin/env python
"""
_HeritageLFNChild_

MySQL implementation of DBSBufferFiles.HeritageLFNChild
"""

__revision__ = "$Id: HeritageLFNChild.py,v 1.1 2009/10/22 15:40:19 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class HeritageLFNChild(DBFormatter):
    sql = """INSERT INTO dbsbuffer_file_parent (child, parent)
               SELECT dbsbuffer_file.id, :parent FROM dbsbuffer_file
                 WHERE dbsbuffer_file.lfn = :lfn"""
    
    def execute(self, childLFNs, parentID, conn = None, transaction = False):
        binds = []
        for childLFN in childLFNs:
            binds.append({"lfn": childLFN, "parent": parentID})
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
