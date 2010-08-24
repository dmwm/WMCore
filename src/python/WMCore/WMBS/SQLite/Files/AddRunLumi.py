"""
SQLite implementation of AddRunLumiFile
"""

from WMCore.WMBS.MySQL.Files.AddRunLumi import AddRunLumi as AddRunLumiMySQL

class AddRunLumi(AddRunLumiMySQL):
    sql = """insert into wmbs_file_runlumi_map (file, run, lumi) 
                values ((select id from wmbs_file_details where lfn = :lfn), :run, :lumi)"""