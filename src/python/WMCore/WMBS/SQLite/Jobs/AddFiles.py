"""
SQLite implementation of Jobs.AddFiles
"""

from WMCore.WMBS.MySQL.Jobs.AddFiles import AddFiles as AddFilesJobMySQL

class AddFiles(AddFilesJobMySQL):
    sql = """INSERT INTO wmbs_job_assoc (job, file)
               SELECT :jobid, :fileid WHERE NOT EXISTS
                 (SELECT * FROM wmbs_job_assoc
                  WHERE job = :jobid AND file = :fileid)"""
