"""
Oracle implementation of GetByID
"""

from WMCore.WMBS.MySQL.Files.GetByID import GetByID as GetFileByIDMySQL

class GetByID(GetFileByIDMySQL):
    """
    _GetByID_
    
    Oracle specific: file and size are reserved words and doesn't allow 'as'
    for table rename
    """
    
    sql = """select fileD.id, fileD.lfn, fileD.filesize, fileD.events, map.run, map.lumi
             from wmbs_file_details fileD 
             inner join wmbs_file_runlumi_map map 
             on (map.fileid = fileD.id) 
             where fileD.id = :fileid"""
             