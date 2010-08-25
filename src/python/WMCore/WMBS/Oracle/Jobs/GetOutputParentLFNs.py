#!/usr/bin/env python
"""
_GetOutputParentLFNs_

Oracle implementation of Jobs.GetOutputParentLFNs
"""




from WMCore.WMBS.MySQL.Jobs.GetOutputParentLFNs import GetOutputParentLFNs as GetOutputParentLFNsMySQL

class GetOutputParentLFNs(GetOutputParentLFNsMySQL):
    inputSQL = """SELECT DISTINCT wmbs_file_details.lfn, wmbs_file_details.merged 
                  FROM wmbs_file_details
                    INNER JOIN wmbs_job_assoc ON
                      wmbs_file_details.id = wmbs_job_assoc.fileid
                  WHERE wmbs_job_assoc.job = :job"""

    parentSQL = """SELECT DISTINCT wmbs_file_details.lfn FROM wmbs_file_details
                     INNER JOIN wmbs_file_parent ON
                       wmbs_file_details.id = wmbs_file_parent.parent
                     INNER JOIN wmbs_job_assoc ON
                       wmbs_file_parent.child = wmbs_job_assoc.fileid 
                   WHERE wmbs_job_assoc.job = :job"""
