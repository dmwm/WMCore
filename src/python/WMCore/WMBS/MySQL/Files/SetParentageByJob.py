#!/usr/bin/env python
"""
MySQL implementation of File.SetParentageByJob

Make the parentage link between a file and all the inputs of a given job
"""

__revision__ = "$Id: SetParentageByJob.py,v 1.2 2010/03/08 16:31:15 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetParentageByJob(DBFormatter):
    """
    Make the parentage link between a file and all the inputs of a given job
    """

    sql = """INSERT INTO wmbs_file_parent (child, parent)
             SELECT DISTINCT wmbs_file_details.id, wmbs_job_assoc.file
             FROM wmbs_file_details, wmbs_job_assoc
             WHERE wmbs_job_assoc.job = :jobid
             AND wmbs_file_details.id = :child
    """
    
    def execute(self, jobID = None, child=0, conn = None, transaction = False):

        result = self.dbi.processData(self.sql, {'child': child, 'jobid': jobID}, 
                         conn = conn, transaction = transaction)

        return self.format(result)
