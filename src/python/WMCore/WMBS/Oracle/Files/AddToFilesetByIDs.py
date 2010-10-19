#!/usr/bin/env python
"""
_AddToFilesByIDs_

Oracle implementation of AddFileToFilesetByIDs
"""

from WMCore.WMBS.MySQL.Files.AddToFilesetByIDs import AddToFilesetByIDs as AddFileToFilesetByIDsMySQL

class AddToFilesetByIDs(AddFileToFilesetByIDsMySQL):
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time) 
               SELECT :file_id, wmbs_fileset.id, :insert_time 
                 FROM wmbs_fileset WHERE wmbs_fileset.id = :fileset"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id AS subscription, :fileid
                           FROM wmbs_subscription
                    WHERE wmbs_subscription.fileset = :fileset"""
