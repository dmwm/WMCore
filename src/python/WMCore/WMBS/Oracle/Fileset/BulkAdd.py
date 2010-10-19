#!/usr/bin/env python
"""
_BulkAdd_

Oracle implementation of Fileset.BulkAdd
"""

from WMCore.WMBS.MySQL.Fileset.BulkAdd import BulkAdd as BulkAddFilesetMySQL

class BulkAdd(BulkAddFilesetMySQL):
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               VALUES (:fileid, :fileset, :timestamp)"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id AS subscription, :fileid AS fileid
                           FROM wmbs_subscription
                    WHERE wmbs_subscription.fileset = :fileset"""
