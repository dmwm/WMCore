"""
Oracle implementation of Files.Heritage
"""

from WMCore.WMBS.MySQL.Files.Heritage import Heritage as HeritageMySQL

class Heritage(HeritageMySQL):
    sql = HeritageMySQL.sql
