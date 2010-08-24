#!/usr/bin/env python
"""
_AddToFileset_

Oracle implementation of Files.AddFileToFileset
"""

from WMCore.WMBS.MySQL.Files.AddToFileset import AddToFileset as AddFileToFilesetMySQL

class AddToFileset(AddFileToFilesetMySQL):
    sql = """INSERT INTO wmbs_fileset_files (fileid, fileset, insert_time)
               SELECT wmbs_file_details.id, :fileset, :insert_time
               FROM wmbs_file_details
               WHERE wmbs_file_details.lfn = :lfn
               AND NOT EXISTS (SELECT fileid FROM wmbs_fileset_files wff2 WHERE
                                wff2.fileid = wmbs_file_details.id
                                AND wff2.fileset = :fileset)
    """    

