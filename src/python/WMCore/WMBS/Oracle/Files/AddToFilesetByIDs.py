"""
Oracle implementation of AddFileToFilesetByIDs
"""
from WMCore.WMBS.MySQL.Files.AddToFilesetByIDs import AddToFilesetByIDs as AddFileToFilesetByIDsMySQL

class AddToFilesetByIDs(AddFileToFilesetByIDsMySQL):
    """
    _AddToFilesetByIDs_
    
    overwirtes MySQL Files.AddToFilesetByIDs.sql to use in oracle.
    
    """
    
    sql = """insert into wmbs_fileset_files 
            (fileid, fileset, insert_time) 
            values (:file_id, :fileset, :insert_time)"""
