"""
_IsCompleteOnRun_
Oracle implementation of Subscriptions.IsCompleteOnRun

Checks all files in the given subscription and given run are completed.
"""
__all__ = []
__revision__ = "$Id: IsCompleteOnRun.py,v 1.1 2009/04/16 18:47:08 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.IsCompleteOnRun import IsCompleteOnRun as IsCompleteOnRunMySQL

class IsCompleteOnRun(IsCompleteOnRunMySQL):
    """
    _IsCompleteOnRun_
    
    Return number of files for available, complete, failed status
    for a given run and a given subscription.
    
    TODO: can use left outer join to check the completeness.
    Not sure join is more expensive than multiple select with count
    """
    sql = """SELECT count(*) FROM wmbs_fileset_files wff
                INNER JOIN wmbs_subscription ws ON (ws.fileset = wff.fileset)
                INNER JOIN wmbs_file_runlumi_map wrm ON (wrm.fileid = wff.fileid)
                LEFT OUTER JOIN wmbs_sub_files_failed wf ON (wf.fileid = wff.fileid)
                LEFT OUTER JOIN wmbs_sub_files_complete wc ON (wc.fileid = wff.fileid)
                WHERE
                 wf.fileid IS NULL AND wc.fileid IS NULL AND
                 ws.id = :subID AND wrm.run = :runID                 
          """