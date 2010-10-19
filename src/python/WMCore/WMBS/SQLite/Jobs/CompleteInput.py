#!/usr/bin/env python
"""
_CompleteInput_

SQLite implementation of Jobs.CompleteInput
"""

from WMCore.WMBS.MySQL.Jobs.CompleteInput import CompleteInput as MySQLCompleteInput

class CompleteInput(MySQLCompleteInput):
    sql = """INSERT INTO wmbs_sub_files_complete (file, subscription)
               SELECT :fileid, :subid WHERE NOT EXISTS
                 (SELECT * FROM wmbs_sub_files_complete
                  WHERE file = :fileid AND subscription = :subid)"""

