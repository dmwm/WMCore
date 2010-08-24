"""
Oracle implementation of GetRunLumiFile
"""

from WMCore.WMBS.MySQL.Files.GetRunLumiFile import GetRunLumiFile as GetRunLumiFileMySQL

class GetRunLumiFile(GetRunLumiFileMySQL):
    sql = """select flr.run as run, flr.lumi as lumi
		from wmbs_file_runlumi_map flr 
			where flr.fileid in (select id from wmbs_file_details where lfn=:lfn)"""
