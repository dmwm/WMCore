#!/usr/bin/env python
"""
_GetAvailableFilesMeta_

Oracle implementation of Subscription.GetAvailableFilesMeta
"""

__revision__ = "$Id: GetAvailableFilesMeta.py,v 1.2 2009/07/24 15:52:39 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesMeta import \
     GetAvailableFilesMeta as GetAvailableFilesMetaMySQL

class GetAvailableFilesMeta(GetAvailableFilesMetaMySQL):
    sql = """SELECT wmbs_file_details.id, wmbs_file_details.lfn, wmbs_file_details.filesize,
                    wmbs_file_details.events, MIN(wmbs_file_runlumi_map.run) AS run
                    FROM wmbs_file_details
               INNER JOIN wmbs_fileset_files
                 ON wmbs_file_details.id = wmbs_fileset_files.fileid
               INNER JOIN wmbs_file_runlumi_map
                 ON wmbs_file_details.id = wmbs_file_runlumi_map.fileid
               INNER JOIN wmbs_subscription
                 ON wmbs_subscription.fileset = wmbs_fileset_files.fileset 
               LEFT OUTER JOIN  wmbs_sub_files_acquired wa
                 ON ( wa.fileid = wmbs_fileset_files.fileid AND wa.subscription = wmbs_subscription.id )
               LEFT OUTER JOIN  wmbs_sub_files_failed wf
                 ON ( wf.fileid = wmbs_fileset_files.fileid AND wf.subscription = wmbs_subscription.id )
               LEFT OUTER JOIN  wmbs_sub_files_complete wc
                 ON ( wc.fileid = wmbs_fileset_files.fileid AND wc.subscription = wmbs_subscription.id )
               WHERE wmbs_subscription.id = :subscription
                 AND wa.fileid is NULL 
                 AND wf.fileid is NULL
                 AND wc.fileid is NULL
               GROUP BY wmbs_file_details.id, wmbs_file_details.lfn, wmbs_file_details.filesize,
                        wmbs_file_details.events
               """
