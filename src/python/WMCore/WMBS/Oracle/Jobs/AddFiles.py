"""
Oracle implementation of Jobs.AddFiles
"""

from WMCore.WMBS.MySQL.Jobs.AddFiles import AddFiles as AddFilesJobMySQL

class AddFiles(AddFilesJobMySQL):
    """
    _AddFiles_
    Oracle specific query: file is a reserved word
    """
    sql = "insert into wmbs_job_assoc (job, fileid) values (:jobid, :fileid)"