#!/usr/bin/env python
"""
_GetParentAndGrandParentInfo_

Figure out parentage information for a file in WMBS.  This will return
information about a file's parent and it's grand parent such as the
lfn, id and whether or not the file is merged.  This will also determine
whether or not the file is a redneck parent or redneck child.
"""
from __future__ import division

from WMCore.Database.DBFormatter import DBFormatter


class GetParentAndGrandParentInfo(DBFormatter):
    sql = """SELECT wfp.id, wfp.lfn, wfp.merged,
                    wfgp.lfn AS gplfn, wfgp.merged AS gpmerged
             FROM wmbs_file_details wfp
             INNER JOIN wmbs_file_parent wfpa ON wfpa.parent = wfp.id
             INNER JOIN wmbs_file_details wfd ON wfd.id = wfpa.child
             LEFT OUTER JOIN wmbs_file_parent wfpb ON wfpb.child = wfp.id
             LEFT OUTER JOIN wmbs_file_details wfgp ON wfgp.id = wfpb.parent
             WHERE wfd.lfn = :child_lfn
    """

    def execute(self, childLFNs, conn=None, transaction=False):
        bindVars = []
        for childLFN in childLFNs:
            bindVars.append({"child_lfn": childLFN})

        result = self.dbi.processData(self.sql, bindVars,
                                      conn=conn, transaction=transaction)
        return self.formatDict(result)
