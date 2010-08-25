#!/usr/bin/env python
"""
_GetFilesForMerge_

MySQL implementation of Subscription.GetFilesForMerge
"""

__all__ = []
__revision__ = "$Id: GetFilesForMerge.py,v 1.6 2010/03/08 17:06:09 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

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
                    MIN(wmbs_file_parent.parent) AS file_parent
             FROM wmbs_file_details
             INNER JOIN wmbs_file_runlumi_map ON
               wmbs_file_details.id = wmbs_file_runlumi_map.file
             INNER JOIN
               (SELECT wmbs_fileset_files.file AS file FROM wmbs_fileset_files
                  INNER JOIN wmbs_subscription ON
                    wmbs_fileset_files.fileset = wmbs_subscription.fileset
                  LEFT OUTER JOIN wmbs_sub_files_acquired ON
                    wmbs_fileset_files.file = wmbs_sub_files_acquired.file AND
                    wmbs_sub_files_acquired.subscription = wmbs_subscription.id
                  LEFT OUTER JOIN wmbs_sub_files_complete ON
                    wmbs_fileset_files.file = wmbs_sub_files_complete.file AND
                    wmbs_sub_files_complete.subscription = wmbs_subscription.id
                  LEFT OUTER JOIN wmbs_sub_files_failed ON
                    wmbs_fileset_files.file = wmbs_sub_files_failed.file AND
                    wmbs_sub_files_failed.subscription = wmbs_subscription.id                    
                  WHERE wmbs_subscription.id = :p_1 AND
                        wmbs_sub_files_acquired.file IS NULL AND
                        wmbs_sub_files_complete.file IS NULL AND
                        wmbs_sub_files_failed.file IS NULL) merge_fileset ON
               wmbs_file_details.id = merge_fileset.file
             LEFT OUTER JOIN wmbs_file_parent ON
               wmbs_file_details.id = wmbs_file_parent.child
             WHERE wmbs_file_details.id NOT IN
               (SELECT child FROM wmbs_file_parent
                  INNER JOIN wmbs_job_assoc ON
                    wmbs_file_parent.parent = wmbs_job_assoc.file
                  INNER JOIN wmbs_job ON
                    wmbs_job_assoc.job = wmbs_job.id
                  INNER JOIN wmbs_jobgroup ON
                    wmbs_job.jobgroup = wmbs_jobgroup.id
                  INNER JOIN wmbs_job_state ON
                    wmbs_job.state = wmbs_job_state.id
                WHERE wmbs_job.outcome = 0 OR
                      wmbs_job_state.name != 'cleanout' AND
                      wmbs_jobgroup.subscription = :p_1)
             GROUP BY wmbs_file_details.id, wmbs_file_details.events,
                    wmbs_file_details.size, wmbs_file_details.lfn,
                    wmbs_file_details.first_event, wmbs_file_runlumi_map.run,
                    wmbs_file_runlumi_map.lumi"""

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
