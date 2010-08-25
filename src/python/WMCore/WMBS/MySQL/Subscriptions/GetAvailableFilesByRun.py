#!/usr/bin/env python
"""
_GetAvailableFilesByRun_

MySQL implementation of Subscription.GetAvailableFilesByRun
"""




from WMCore.Database.DBFormatter import DBFormatter

class GetAvailableFilesByRun(DBFormatter):
    sql = """SELECT distinct(wff.file) FROM wmbs_fileset_files wff 
               INNER JOIN wmbs_subscription ws ON ws.fileset = wff.fileset
               INNER JOIN wmbs_file_runlumi_map wm ON (wm.file = wff.file) 
               INNER JOIN wmbs_file_location wfl ON wfl.file = wff.file
               LEFT OUTER JOIN  wmbs_sub_files_acquired wa ON ( wa.file = wff.file AND wa.subscription = ws.id )
               LEFT OUTER JOIN  wmbs_sub_files_failed wf ON ( wf.file = wff.file AND wf.subscription = ws.id )
               LEFT OUTER JOIN  wmbs_sub_files_complete wc ON ( wc.file = wff.file AND wc.subscription = ws.id )
             WHERE ws.id=:subscription AND wm.run = :run AND wa.file is NULL 
                   AND wf.file is NULL AND wc.file is NULL"""

    def formatDict(self, results):
        """
        _formatDict_

        Cast the file column to an integer as the DBFormatter's formatDict()
        method turns everything into strings.  Also, fixup the results of the
        Oracle query by renaming "fileid" to file.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        for formattedResult in formattedResults:
            if "file" in formattedResult.keys():
                formattedResult["file"] = int(formattedResult["file"])
            else:
                formattedResult["file"] = int(formattedResult["fileid"])

        return formattedResults
           
    def execute(self, subscription, run, conn = None, transaction = False):
        binds = {'subscription': subscription, 'run': run}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)
