#!/usr/bin/env python
"""
_GetT0PromptRecoAvailableFiles_

Oracle implementation of Subscription.GetT0PromptRecoAvailableFiles
"""

__revision__ = "$Id: GetT0PromptRecoAvailableFiles.py,v 1.2 2009/10/27 09:03:43 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.GetT0PromptRecoAvailableFiles import GetT0PromptRecoAvailableFiles \
     as GetT0PromptRecoAvailableFilesMySQL

class GetT0PromptRecoAvailableFiles(GetT0PromptRecoAvailableFilesMySQL):
    sql = """SELECT wmbs_fileset_files.fileid FROM wmbs_fileset_files
               INNER JOIN wmbs_subscription USING(fileset)
               INNER JOIN wmbs_file_runlumi_map ON
                 wmbs_file_runlumi_map.fileid = wmbs_fileset_files.fileid
               INNER JOIN run ON
                 run.run_id = wmbs_file_runlumi_map.run
               LEFT OUTER JOIN wmbs_sub_files_acquired ON
                 wmbs_sub_files_acquired.fileid = wmbs_fileset_files.fileid AND wmbs_sub_files_acquired.subscription = wmbs_subscription.id
               LEFT OUTER JOIN wmbs_sub_files_failed ON
                 wmbs_sub_files_failed.fileid = wmbs_fileset_files.fileid AND wmbs_sub_files_failed.subscription = wmbs_subscription.id
               LEFT OUTER JOIN wmbs_sub_files_complete ON
                 wmbs_sub_files_complete.fileid = wmbs_fileset_files.fileid AND wmbs_sub_files_complete.subscription = wmbs_subscription.id
             WHERE wmbs_subscription.id = :subscription
               AND wmbs_sub_files_acquired.fileid is NULL
               AND wmbs_sub_files_failed.fileid is NULL
               AND wmbs_sub_files_complete.fileid is NULL
               AND run.reco_started = 1
             """
