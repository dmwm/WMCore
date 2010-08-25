#!/usr/bin/env python
"""
_GetAvailableFiles_

Oracle implementation of Subscription.GetAvailableFiles
"""



from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import \
     GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL):
    sql = """SELECT wff.fileid, wl.se_name FROM wmbs_fileset_files wff 
               INNER JOIN wmbs_subscription ws ON ws.fileset = wff.fileset 
               INNER JOIN wmbs_file_location wfl ON wfl.fileid = wff.fileid
               INNER JOIN wmbs_location wl ON wl.id = wfl.location 
               LEFT OUTER JOIN  wmbs_sub_files_acquired wa ON ( wa.fileid = wff.fileid AND wa.subscription = ws.id )
               LEFT OUTER JOIN  wmbs_sub_files_failed wf ON ( wf.fileid = wff.fileid AND wf.subscription = ws.id )
               LEFT OUTER JOIN  wmbs_sub_files_complete wc ON ( wc.fileid = wff.fileid AND wc.subscription = ws.id )
               WHERE ws.id=:subscription AND wa.fileid is NULL 
                     AND wf.fileid is NULL AND wc.fileid is NULL    
              """
