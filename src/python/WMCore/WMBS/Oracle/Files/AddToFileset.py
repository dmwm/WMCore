"""
Oracle implementation of AddFileToFileset
"""
from WMCore.WMBS.MySQL.Files.AddToFileset import AddToFileset as AddFileToFilesetMySQL

class AddToFileset(AddFileToFilesetMySQL):
    """
    _AddToFileset_
    
    overwirtes MySQL Files.AddToFilesetByID.sql to use in oracle.
    
    """
    
    sql = """insert into wmbs_fileset_files 
            (fileid, fileset, insert_time) 
            values ((select id from wmbs_file_details where lfn = :lfn),
            (select id from wmbs_fileset where name = :fileset), :insert_time)"""
