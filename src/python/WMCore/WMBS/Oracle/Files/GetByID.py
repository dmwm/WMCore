"""
Oracle implementation of GetByID
"""

from WMCore.WMBS.MySQL.Files.GetByID import GetByID as GetByIDMySQL

class GetByID(GetByIDMySQL):
    sql = """SELECT id, lfn, filesize, events, cksum, first_event, last_event, merged
             FROM wmbs_file_details WHERE id = :fileid"""
