"""
SQLite implementation of Jobs.AddFiles
"""

from WMCore.WMBS.MySQL.Jobs.AddFiles import AddFiles as AddFilesJobMySQL

class AddFiles(AddFilesJobMySQL):
    sql = AddFilesJobMySQL.sql