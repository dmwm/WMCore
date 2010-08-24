"""
Oracle implementation of GetByID
"""

from WMCore.WMBS.MySQL.Files.GetByID import GetByID as GetByIDMySQL

class GetByID(GetByIDMySQL):
    """
    _GetByID_
    
    """
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.lfn,
             wmbs_file_details.filesize, wmbs_file_details.events,
             wmbs_file_details.cksum FROM wmbs_file_details
             WHERE wmbs_file_details.id = :fileid"""
