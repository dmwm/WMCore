#!/usr/bin/env python
"""
_GetT0PromptRecoAvailableFiles_

MySQL implementation of Subscription.GetT0PromptRecoAvailableFiles

Return a list of files that are available for processing.  This is used for
PromptReco in the T0 and will query T0 specific tables to determine if a run
has been released for PromptReco.
"""

__all__ = []
__revision__ = "$Id: GetT0PromptRecoAvailableFiles.py,v 1.2 2009/10/27 09:03:43 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetT0PromptRecoAvailableFiles(DBFormatter):
    sql = """SELECT wmbs_fileset_files.file FROM wmbs_fileset_files
               INNER JOIN wmbs_subscription USING(fileset)
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_file_runlumi_map.file = wmbs_fileset_files.file
               INNER JOIN run ON
                 run.run_id = wmbs_file_runlumi_map.run
               LEFT OUTER JOIN wmbs_sub_files_acquired ON
                 wmbs_sub_files_acquired.file = wmbs_fileset_files.file AND wmbs_sub_files_acquired.subscription = wmbs_subscription.id
               LEFT OUTER JOIN wmbs_sub_files_failed ON
                 wmbs_sub_files_failed.file = wmbs_fileset_files.file AND wmbs_sub_files_failed.subscription = wmbs_subscription.id
               LEFT OUTER JOIN wmbs_sub_files_complete ON
                 wmbs_sub_files_complete.file = wmbs_fileset_files.file AND wmbs_sub_files_complete.subscription = wmbs_subscription.id
             WHERE wmbs_subscription.id = :subscription
               AND wmbs_sub_files_acquired.file is NULL
               AND wmbs_sub_files_failed.file is NULL
               AND wmbs_sub_files_complete.file is NULL
               AND run.reco_started = 1
             """
        
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
           
    def execute(self, subscription = None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)
