from __future__ import print_function, division
from WMCore.Database.DBFormatter import DBFormatter

class CheckDrainStatus(DBFormatter):
    """
    _CheckDrainStatus_
    
    1. checks all the workflows are completed status (within the agent)
    2. checks all the blocks are closed
    3. checks all the files are injected to PhEDEx
    4. checks all the files are uploaded to DBS

    TODO: need to check archived statsus
    """
    completedSQL = "SELECT count(*) FROM dbsbuffer_workflow WHERE completed = 0"
    
    blockClosedSQL = "SELECT count(*) FROM dbsbuffer_block where status != 'Closed'"

    phedexSQL = "SELECT count(*) FROM dbsbuffer_file WHERE in_phedex = 0"
    
    dbsSQL = "SELECT count(*) FROM dbsbuffer_file WHERE status != 'InDBS'"

    def _executeQuery(self, sql, keyName, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        count = self.format(result)[0]
        return {keyName: count}
        
    def execute(self, conn = None, transaction = False):
        result = {}
        # number of workflows
        completeStatus = self._executeQuery(self.completedSQL, "NotCompleted", conn, transaction)
        result.update(completeStatus)
        
        # number of blocks
        closedStatus = self._executeQuery(self.completedSQL, "NotClosed", conn, transaction)
        result.update(closedStatus)
        
        # number of files
        phedexStatus = self._executeQuery(self.completedSQL, "NotInPhEDEx", conn, transaction)
        result.update(phedexStatus)
        
        # number of files
        dbsStatus = self._executeQuery(self.completedSQL, "NotInDBS", conn, transaction)
        result.update(dbsStatus)

        return result
