#!/usr/bin/env python
"""
_GetAvailableFilesID_

Oracle implementation of Subscription.GetAvailableFilesID
"""



from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFilesID import \
     GetAvailableFilesID as GetAvailableFilesIDMySQL

class GetAvailableFilesID(GetAvailableFilesIDMySQL):
    sql = """SELECT wff.fileid AS fileid FROM wmbs_fileset_files wff 
               INNER JOIN wmbs_subscription ws ON ws.fileset = wff.fileset 
               LEFT OUTER JOIN  wmbs_sub_files_acquired wa ON ( wa.fileid = wff.fileid AND wa.subscription = ws.id )
               LEFT OUTER JOIN  wmbs_sub_files_failed wf ON ( wf.fileid = wff.fileid AND wf.subscription = ws.id )
               LEFT OUTER JOIN  wmbs_sub_files_complete wc ON ( wc.fileid = wff.fileid AND wc.subscription = ws.id )
               WHERE ws.id=:subscription AND wa.fileid is NULL 
                     AND wf.fileid is NULL AND wc.fileid is NULL    
              """
