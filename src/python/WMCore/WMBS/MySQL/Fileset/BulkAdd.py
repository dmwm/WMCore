"""
_BulkAdd_

MySQL implementation of Fileset.BulkAdd
"""

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
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               VALUES (:fileid, :fileset, :timestamp)"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id AS subscription, :fileid
                           FROM wmbs_subscription
                    WHERE wmbs_subscription.fileset = :fileset"""

    def execute(self, binds, conn = None, transaction = False):
        timestamp = int(time.time())
        newBinds = []
        for bind in binds:
            newBind = {"timestamp": timestamp}
            newBind.update(bind)
            newBinds.append(newBind)

        self.dbi.processData(self.sql, newBinds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.sqlAvail, binds, conn = conn,
                             transaction = transaction)
        return
