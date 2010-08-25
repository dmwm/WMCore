#!/usr/bin/env python
"""
_GetFilesForMerge_

MySQL implementation of Subscription.GetFilesForMerge
"""

__all__ = []
__revision__ = "$Id: GetFilesForMerge.py,v 1.3 2009/08/27 19:31:29 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetFilesForMerge(DBFormatter):
    """
    This query needs to return the following for any files that is deemed
    mergeable:
      WMBS ID (file_id)
      Events (file_events)
      Size (file_size)
      LFN (file_lfn)
      First event in file (file_first_event)
      Runs in file (file_run)
      Lumi sections in file (file_lumi)
      WMBS JobGroup ID that produced the file (group_id)

    The query should return a row with every run, lumi combination for a
    particular file.  The formatDict() method in this class will go through
    the results and prune out all rows for a file except the one with the
    lowest run/lumi combination.  A file is deemed mergeable if:
      - The file is in the input fileset for the merging subscription
      - It has not been acquired/completed/failed by it's subscription
      - The JobGroup that produced it is "COMPLETE"      
    """
    sql = """SELECT wmbs_file_details.id AS file_id,
                    wmbs_file_details.events AS file_events,
                    wmbs_file_details.size AS file_size,
                    wmbs_file_details.lfn AS file_lfn,
                    wmbs_file_details.first_event AS file_first_event,
                    wmbs_file_runlumi_map.run AS file_run,
                    wmbs_file_runlumi_map.lumi AS file_lumi,
                    wmbs_jobgroup.id AS group_id
             FROM wmbs_file_details
             INNER JOIN wmbs_file_runlumi_map
               ON wmbs_file_details.id = wmbs_file_runlumi_map.file
             INNER JOIN wmbs_fileset_files
               ON wmbs_file_details.id = wmbs_fileset_files.file
             INNER JOIN wmbs_jobgroup
               ON wmbs_fileset_files.fileset = wmbs_jobgroup.output
             WHERE wmbs_file_details.id IN
               (SELECT file FROM wmbs_fileset_files INNER JOIN wmbs_subscription
                  ON wmbs_fileset_files.fileset = wmbs_subscription.fileset
                WHERE wmbs_subscription.id = :p_1)
               AND wmbs_jobgroup.id NOT IN
                 (SELECT jobgroup FROM
                   (SELECT wmbs_job.jobgroup AS jobgroup , COUNT(*) AS total FROM wmbs_job
                      INNER JOIN wmbs_job_state ON wmbs_job.state = wmbs_job_state.id
                    WHERE wmbs_job.outcome = 0 OR wmbs_job_state.name != 'complete'
                    GROUP BY wmbs_job.jobgroup) incomplete
                  WHERE incomplete.total != 0)  
               AND NOT EXISTS
                 (SELECT * FROM wmbs_sub_files_acquired WHERE
                   wmbs_file_details.id = wmbs_sub_files_acquired.file AND
                   :p_1 = wmbs_sub_files_acquired.subscription)
               AND NOT EXISTS
                 (SELECT * FROM wmbs_sub_files_complete WHERE
                   wmbs_file_details.id = wmbs_sub_files_complete.file AND
                   :p_1 = wmbs_sub_files_complete.subscription)
               AND NOT EXISTS
                 (SELECT * FROM wmbs_sub_files_failed WHERE
                   wmbs_file_details.id = wmbs_sub_files_failed.file AND
                   :p_1 = wmbs_sub_files_failed.subscription)
             """

    def formatFileInfo(self, fileInfo):
        """
        _formatFileInfo_

        Given a fileInfo dictionary convert the values for the following keys
        back to integers as the database code returns everything as strings:
          file_events, file_size, file_first_event, file_lumi, file_run
        """
        fileInfo["file_events"] = int(fileInfo["file_events"])
        fileInfo["file_size"] = int(fileInfo["file_size"])        
        fileInfo["file_first_event"] = int(fileInfo["file_first_event"])
        fileInfo["file_lumi"] = int(fileInfo["file_lumi"])
        fileInfo["file_run"] = int(fileInfo["file_run"])
        return fileInfo
        
    def formatDict(self, results):
        """
        _formatDict_

        Format the results of the query.  This will prune out duplicate rows
        for files that belong to more than one run/lumi section.  The results
        will be returned in the form of a list of dictionaries with one
        dictionary per file.  The lowest run/lumi combination for a file will
        be associated with the file.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        mergeableFiles = []
        for formattedResult in formattedResults:
            formattedResult = self.formatFileInfo(formattedResult)
            
            for mergeableFile in mergeableFiles:
                if mergeableFile["file_id"] == formattedResult["file_id"]:
                    if formattedResult["file_run"] < mergeableFile["file_run"] or \
                       (formattedResult["file_run"] == mergeableFile["file_run"] and \
                        formattedResult["file_lumi"] < mergeableFile["file_lumi"]):
                        mergeableFile["file_run"] = formattedResult["file_run"]
                        mergeableFile["file_lumi"] = formattedResult["file_lumi"]

                    break
            else:
                newMergeableFile = {}
                for key in formattedResult.keys():
                    newMergeableFile[key] = formattedResult[key]

                mergeableFiles.append(newMergeableFile)
                
        return mergeableFiles

    def execute(self, subscription = None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"p_1": subscription}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)
