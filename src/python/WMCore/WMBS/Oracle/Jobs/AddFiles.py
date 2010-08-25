"""
_AddFiles_

Oracle implementation of Jobs.AddFiles
"""

from WMCore.WMBS.MySQL.Jobs.AddFiles import AddFiles as AddFilesJobMySQL

class AddFiles(AddFilesJobMySQL):
    """
    _AddFiles_
    
    Oracle specific query: file is a reserved word
    """
    sql = """INSERT INTO wmbs_job_assoc (job, fileid)
               SELECT :jobid, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT * FROM wmbs_job_assoc
                  WHERE job = :jobid AND file = :fileid)"""    
