#!/usr/bin/env python
"""
_GetAvailableFilesByRun_

Oracle implementation of Subscription.GetAvailableFilesByRun
"""




from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesByRun import \
     GetAvailableFilesByRun as GetAvailableFilesByRunMySQL

class GetAvailableFilesByRun(GetAvailableFilesByRunMySQL):
    sql = """SELECT distinct(wff.fileid) FROM wmbs_fileset_files wff 
               INNER JOIN wmbs_subscription ws ON ws.fileset = wff.fileset
               INNER JOIN wmbs_file_runlumi_map wm ON (wm.fileid = wff.fileid)  
               INNER JOIN wmbs_file_location wfl ON wfl.fileid = wff.fileid
               LEFT OUTER JOIN  wmbs_sub_files_acquired wa ON ( wa.fileid = wff.fileid AND wa.subscription = ws.id )
               LEFT OUTER JOIN  wmbs_sub_files_failed wf ON ( wf.fileid = wff.fileid AND wf.subscription = ws.id )
               LEFT OUTER JOIN  wmbs_sub_files_complete wc ON ( wc.fileid = wff.fileid AND wc.subscription = ws.id )
               WHERE ws.id=:subscription AND wm.run = :run AND wa.fileid is NULL 
                     AND wf.fileid is NULL AND wc.fileid is NULL    
              """
