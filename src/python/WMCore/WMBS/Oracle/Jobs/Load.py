"""
Oracle implementation of Jobs.Load
"""
from WMCore.WMBS.MySQL.Jobs.Load import Load as LoadJobMySQL

class Load(LoadJobMySQL):
    sql = LoadJobMySQL.sql