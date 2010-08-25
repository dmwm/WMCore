#!/usr/bin/env python
"""
MySQL implementation of File.SetParentageByJob

Make the parentage link between a file and all the inputs of a given job
"""
__all__ = []
__revision__ = "$Id: SetParentageByJob.py,v 1.1 2010/02/26 20:46:06 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SetParentageByJob(DBFormatter):
    """
    
    Make the parentage link between a file and all the inputs of a given job

    """
    
    sql = """insert into wmbs_file_parent (child, parent) values (
                SELECT (:child, (SELECT DISTINCT FILE FROM wmbs_job_assoc WHERE JOB = :jobid) FROM dual) )"""

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
