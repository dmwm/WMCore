#!/usr/bin/env python
"""
MySQL implementation of Jobs.LoadOutputID

Load the ID of the output fileset for a job
"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class LoadOutputID(DBFormatter):
    """

    Load the ID of the output fileset for a job

    """


    sql = """SELECT wfs.id AS id FROM wmbs_fileset wfs
                INNER JOIN wmbs_jobgroup wjg ON wjg.output = wfs.id
                INNER JOIN wmbs_job wj ON wj.jobgroup = wjg.id
                WHERE wj.id = :jobid"""


    def execute(self, jobID, conn = None, transaction = False):
        """
        Given a jobID, find the fileset

        """

        result = self.dbi.processData(self.sql, {"jobid": jobID}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)[0].get('id', None)
