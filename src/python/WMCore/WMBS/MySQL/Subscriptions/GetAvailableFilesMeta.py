#!/usr/bin/env python
"""
_GetAvailableFilesMeta_

MySQL implementation of Subscription.GetAvailableFilesMeta.  This differs from
the GetAvailabileFiles DAO in that it returns meta data about that file instead
of just its ID.
"""

__revision__ = "$Id: GetAvailableFilesMeta.py,v 1.1 2009/07/23 20:51:36 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetAvailableFilesMeta(DBFormatter):
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.lfn, wmbs_file_details.size,
                    wmbs_file_details.events FROM wmbs_file_details
               INNER JOIN wmbs_fileset_files
                 ON wmbs_file_details.id = wmbs_fileset_files.file
               INNER JOIN wmbs_subscription
                 ON wmbs_subscription.fileset = wmbs_fileset_files.fileset 
               LEFT OUTER JOIN  wmbs_sub_files_acquired wa
                 ON ( wa.file = wmbs_fileset_files.file AND wa.subscription = wmbs_subscription.id )
               LEFT OUTER JOIN  wmbs_sub_files_failed wf
                 ON ( wf.file = wmbs_fileset_files.file AND wf.subscription = wmbs_subscription.id )
               LEFT OUTER JOIN  wmbs_sub_files_complete wc
                 ON ( wc.file = wmbs_fileset_files.file AND wc.subscription = wmbs_subscription.id )
               WHERE wmbs_subscription.id = :subscription
                 AND wa.file is NULL 
                 AND wf.file is NULL
                 AND wc.file is NULL    
    """
        
    def formatDict(self, results):
        """
        _formatDict_

        Cast the file column to an integer as the DBFormatter's formatDict()
        method turns everything into strings.  Also, fixup the results of the
        Oracle query by renaming "filesize" to size.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        for formattedResult in formattedResults:
            if "size" in formattedResult.keys():
                formattedResult["size"] = int(formattedResult["size"])
            else:
                formattedResult["size"] = int(formattedResult["filesize"])
                del formattedResult["filesize"]

        return formattedResults
           
    def execute(self, subscription = None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)
