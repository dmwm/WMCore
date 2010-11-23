"""
Oracle implementation of AddRunLumiFile
"""
from WMCore.WMBS.MySQL.Files.AddRunLumi import AddRunLumi as AddRunLumiMySQL

class AddRunLumi(AddRunLumiMySQL):
    """
    _AddRunLumi_
    
    overwirtes MySQL Files.AddRunLumi.sql to use in oracle.
    
    To Check: might not need to overwrite if modifiy MySQL.Files.AddRunLumi.sql
    a bit to work with both  
    """
    sql = """INSERT INTO wmbs_file_runlumi_map (fileid, run, lumi) 
                VALUES ((SELECT id from wmbs_file_details WHERE lfn = :lfn), 
                :run, :lumi)"""
