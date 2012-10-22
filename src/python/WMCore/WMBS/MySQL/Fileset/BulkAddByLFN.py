"""
_BulkAddByLFN_

MySQL implementation of Fileset.BulkAddByLFN
"""

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
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               SELECT id, :fileset, :timestamp FROM wmbs_file_details WHERE lfn = :lfn"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id AS subscription,
                           wmbs_file_details.id AS file FROM wmbs_subscription
                      INNER JOIN wmbs_file_details ON
                        wmbs_file_details.lfn = :lfn
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
