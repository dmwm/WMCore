"""
_BulkAddByLFN_

MySQL implementation of Fileset.BulkAddByLFN
"""

__revision__ = "$Id: BulkAddByLFN.py,v 1.2 2010/03/23 20:12:06 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

import time

from WMCore.Database.DBFormatter import DBFormatter

class BulkAddByLFN(DBFormatter):
    """
    _BulkAddByLFN_

    Bulk add multiple files to mupltiple filesets.  The file/fileset mappings
    are passed in as a list of dicts where each dict has two keys:
      lfn
      fileset
    """
    sql = """INSERT INTO wmbs_fileset_files (file, fileset, insert_time)
               SELECT id, :fileset, :timestamp FROM wmbs_file_details WHERE lfn = :lfn"""    
    
    def execute(self, binds, conn = None, transaction = False):
        timestamp = int(time.time())
        newBinds = []
        for bind in binds:
            bind["timestamp"] = timestamp
            newBinds.append(bind)

        self.dbi.processData(self.sql, newBinds, conn = conn,
                             transaction = transaction)

        return
