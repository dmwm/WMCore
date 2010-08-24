"""
MySQL implementation of File.Get
"""
from WMCore.WMBS.MySQL.Files.GetByID import GetByID

class GetByLFN(GetByID):
    sql = """select file.id, file.lfn, file.size, file.events, map.run, map.lumi
             from wmbs_file_details as file join wmbs_file_runlumi_map as map on map.file = file.id 
             where lfn = :fileid"""
    #select id, lfn, size, events, run, lumi from wmbs_file_details where id = :file