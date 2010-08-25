#!/usr/bin/env python
"""
_GetOutputParentLFNs_

MySQL implementation of Jobs.GetOutputParentLFNs
"""

__revision__ = "$Id: GetOutputParentLFNs.py,v 1.1 2009/08/21 10:05:04 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetOutputParentLFNs(DBFormatter):
    inputSQL = """SELECT wmbs_file_details.lfn, wmbs_file_details.merged 
                  FROM wmbs_file_details
                    INNER JOIN wmbs_job_assoc ON
                      wmbs_file_details.id = wmbs_job_assoc.file
                  WHERE wmbs_job_assoc.job = :job"""

    parentSQL = """SELECT wmbs_file_details.lfn FROM wmbs_file_details
                     INNER JOIN wmbs_file_parent ON
                       wmbs_file_details.id = wmbs_file_parent.parent
                     INNER JOIN wmbs_job_assoc ON
                       wmbs_file_parent.child = wmbs_job_assoc.file 
                   WHERE wmbs_job_assoc.job = :job""" 
                       
    def format(self, results):
        """
        _format_

        Format the result of the query so that it is just a single list of LFNs.
        """
        lfns = []
        for result in results:
            lfns.append(result[0])

        return lfns

    def execute(self, job, conn = None, transaction = False):
        result = self.dbi.processData(self.inputSQL, {"job": job}, conn = conn,
                                      transaction = transaction)

        if len(result) == 0:
            return []

        if result[0][1] == 0:
            # The input to the job consisted of unmerged files, so we'll need
            # to query for the parents of the job's input.
            result = self.dbi.processData(self.parentSQL, {"job": job}, conn = conn,
                                          transaction = transaction)

        return self.format(result)
