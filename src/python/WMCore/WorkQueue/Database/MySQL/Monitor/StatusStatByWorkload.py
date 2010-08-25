"""
WMCore/WorkQueue/Database/MySQL/Monitor/StatusStatisctics.py


DAO object for WorkQueue

"""

__all__ = []
__revision__ = "$Id: StatusStatByWorkload.py,v 1.1 2010/05/19 16:08:28 sryu Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class StatusStatByWorkload(DBFormatter):
    
    sql = """SELECT ws.id, ws.name, we.status, count(we.id) as count   
             FROM wq_element we
             INNER JOIN wq_wmtask wt ON (wt.id = we.wmtask_id)
             INNER JOIN wq_wmspec ws ON (ws.id = wt.wmspec_id)
          """
    
    def convertStatus(self, data):
        """
        Take data and convert status number to string
        TODO: overwrite formatDict to prevent this loop.
        """
        #total = 0
        for item in data:
            item.update(status = States[item['status']])
            #total += item['number']
        
        #for item in data:
        #    item.update(total = total)
        
    def execute(self, workloadID = None, conn = None, transaction = False):
        if workloadID == None or workloadID == '*':
            self.sql = "%s GROUP BY ws.id, we.status" % self.sql
            binds = {}
        else:
            self.sql = "%s WHERE ws.id = :workloadID GROUP BY we.status" % self.sql 
            binds = {'workloadID': workloadID}
            
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)
        formResults = self.formatDict(results)
        self.convertStatus(formResults)
        return formResults
