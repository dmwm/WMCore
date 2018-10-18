"""
_GetParentInfo_

Figure out parentage information for a file in WMBS.  This will return
information about a file's parent such as the
lfn, id and whether or not the file is merged.
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetParentInfo(DBFormatter):
    sql = """SELECT wfp.id, wfp.lfn, wfp.merged
             FROM wmbs_file_details wfp
             INNER JOIN wmbs_file_parent wfpa ON wfpa.parent = wfp.id
             INNER JOIN wmbs_file_details wfd ON wfd.id = wfpa.child
             WHERE wfd.lfn = :child_lfn
    """

    def execute(self, childLFNs, conn=None, transaction=False):
        bindVars = []
        for childLFN in childLFNs:
            bindVars.append({"child_lfn": childLFN})

        result = self.dbi.processData(self.sql, bindVars,
                         conn=conn, transaction=transaction)
        return self.formatDict(result)
