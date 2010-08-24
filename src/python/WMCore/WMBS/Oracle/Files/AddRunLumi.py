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
    sql = """insert into wmbs_file_runlumi_map (fileid, run, lumi) 
                values ((select id from wmbs_file_details where lfn = :lfn), 
                        :run, :lumi)"""