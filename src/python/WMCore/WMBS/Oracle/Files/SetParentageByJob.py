#!/usr/bin/env python
"""
Oracle implementation of File.SetParentageByJob

Make the parentage link between a file and all the inputs of a given job
"""




from WMCore.WMBS.MySQL.Files.SetParentageByJob import SetParentageByJob as MySQLSetParentageByJob

class SetParentageByJob(MySQLSetParentageByJob):
    sql = """INSERT INTO wmbs_file_parent (child, parent)
             SELECT DISTINCT wmbs_file_details.id, wmbs_job_assoc.fileid
             FROM wmbs_file_details, wmbs_job_assoc
             WHERE wmbs_job_assoc.job = :jobid
             AND wmbs_file_details.lfn = :child
             """
