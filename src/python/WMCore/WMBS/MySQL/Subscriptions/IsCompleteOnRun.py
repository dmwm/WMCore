"""
_IsCompleteOnRun_
MySQL implementation of Subscriptions.IsCompleteOnRun

Checks all files in the given subscription and given run are completed.
"""
__all__ = []
__revision__ = "$Id: IsCompleteOnRun.py,v 1.2 2009/05/27 13:37:50 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class IsCompleteOnRun(DBFormatter):
    """
    _IsCompleteOnRun_
    
    Return number of files for available, complete, failed status
    for a given run and a given subscription.
    
    TODO: can use left outer join to check the completeness.
    Not sure join is more expensive than multiple select with count
    """
    sql = """SELECT count(*) FROM wmbs_fileset_files wff
                INNER JOIN wmbs_subscription ws ON (ws.fileset = wff.fileset)
                INNER JOIN wmbs_file_runlumi_map wrm ON (wrm.file = wff.file)
                LEFT OUTER JOIN wmbs_sub_files_failed wf ON (wf.file = wff.file AND wf.subscription = ws.id)
                LEFT OUTER JOIN wmbs_sub_files_complete wc ON (wc.file = wff.file AND wc.subscription = ws.id)
                WHERE
                 wf.file IS NULL AND wc.file IS NULL AND
                 ws.id = :subID AND wrm.run = :runID                 
          """

    def format(self, result):
        result = DBFormatter.format(self, result)
        return result[0][0]
        
    def execute(self, subID, runID, conn = None, transaction = False):
        binds = self.getBinds(subID=subID, runID=runID)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
