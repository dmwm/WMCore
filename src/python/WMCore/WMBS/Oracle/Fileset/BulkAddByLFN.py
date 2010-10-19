#!/usr/bin/env python
"""
_BulkAddByLFN_

Oracle implementation of Fileset.BulkAddByLFN
"""

from WMCore.WMBS.MySQL.Fileset.BulkAddByLFN import BulkAddByLFN as MySQLBulkAddByLFN

class BulkAddByLFN(MySQLBulkAddByLFN):
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               SELECT (SELECT id FROM wmbs_file_details WHERE lfn = :lfn), :fileset, :timestamp FROM DUAL"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id AS subscription,
                           wmbs_file_details.id AS fileid FROM wmbs_subscription
                      INNER JOIN wmbs_file_details ON
                        wmbs_file_details.lfn = :lfn
                    WHERE wmbs_subscription.fileset = :fileset"""
