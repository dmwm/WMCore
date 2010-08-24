"""
Oracle implementation of GetRunLumiFile
"""

from WMCore.WMBS.MySQL.Files.GetRunLumiFile import GetRunLumiFile as GetRunLumiFileMySQL

class GetRunLumiFile(GetRunLumiFileMySQL):
    sql = GetRunLumiFileMySQL.sql


