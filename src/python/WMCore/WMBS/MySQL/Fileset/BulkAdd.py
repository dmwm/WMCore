"""
_BulkAdd_

MySQL implementation of Fileset.BulkAdd
"""

__revision__ = "$Id: BulkAdd.py,v 1.1 2009/10/14 13:39:32 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class BulkAdd(DBFormatter):
    """
    _BulkAdd_

    Bulk add multiple files to mupltiple filesets.  The file/fileset mappings
    are passed in as a list of dicts where each dict has two keys:
      fileid
      fileset
    """
    sql = """INSERT INTO wmbs_fileset_files (file, fileset, insert_time)
               VALUES (:fileid, :fileset, :timestamp)"""
            
    def execute(self, binds, conn = None, transaction = False):
        timestamp = int(time.time())
        newBinds = []
        for bind in binds:
            bind["timestamp"] = timestamp
            newBinds.append(bind)

        self.dbi.processData(self.sql, newBinds, conn = conn,
                             transaction = transaction)
        return
