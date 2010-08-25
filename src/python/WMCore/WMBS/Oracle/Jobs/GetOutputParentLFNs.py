#!/usr/bin/env python
"""
_GetOutputParentLFNs_

Oracle implementation of Jobs.GetOutputParentLFNs
"""

__revision__ = "$Id: GetOutputParentLFNs.py,v 1.1 2009/08/21 11:08:38 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetOutputParentLFNs import GetOutputParentLFNs as GetOutputParentLFNsMySQL

class GetOutputParentLFNs(GetOutputParentLFNsMySQL):
    inputSQL = """SELECT wmbs_file_details.lfn, wmbs_file_details.merged 
                  FROM wmbs_file_details
                    INNER JOIN wmbs_job_assoc ON
                      wmbs_file_details.id = wmbs_job_assoc.fileid
                  WHERE wmbs_job_assoc.job = :job"""

    parentSQL = """SELECT wmbs_file_details.lfn FROM wmbs_file_details
                     INNER JOIN wmbs_file_parent ON
                       wmbs_file_details.id = wmbs_file_parent.parent
                     INNER JOIN wmbs_job_assoc ON
                       wmbs_file_parent.child = wmbs_job_assoc.fileid 
                   WHERE wmbs_job_assoc.job = :job"""
