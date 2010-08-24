"""
Oracle implementation of GetFileByLFN
"""
from WMCore.WMBS.MySQL.Files.GetByLFN import GetByLFN as GetByLFNMySQL

class GetByLFN(GetByLFNMySQL):
    sql = """SELECT id, lfn, filesize, events, cksum
             FROM wmbs_file_details WHERE lfn = :lfn"""
