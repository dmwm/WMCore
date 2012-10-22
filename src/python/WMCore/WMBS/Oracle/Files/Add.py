"""
Oracle implementation of AddFile
"""
from WMCore.WMBS.MySQL.Files.Add import Add as AddFileMySQL

class Add(AddFileMySQL):
    """
    _Add_

    overwirtes MySQL Files.Add.sql to use oracle sequence instead of auto
    increments
    """
    sql = """INSERT INTO wmbs_file_details (id, lfn, filesize, events,
                                            first_event, merged)
             VALUES (wmbs_file_details_SEQ.nextval, :lfn, :filesize, :events,
                     :first_event, :merged)"""
