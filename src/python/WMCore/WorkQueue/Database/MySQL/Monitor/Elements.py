"""
WMCore/WorkQueue/Database/MySQL/Monitor/Elements.py

DAO object for WorkQueue
"""

__all__ = []
__revision__ = "$Id: Elements.py,v 1.1 2010/02/03 17:20:48 maxa Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.WebTools.DASRESTFormatter import DASRESTFormatter 
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

"""
WorkQueue database structure:
WMCore/WorkQueue/Database/CreateWorkQueueBase.py

wq_element table:
CREATE TABLE wq_element (
   id               INTEGER    NOT NULL,
   wmtask_id        INTEGER    NOT NULL,
   input_id         INTEGER,
   parent_queue_id  INTEGER,
   child_queue      INTEGER,
   num_jobs         INTEGER    NOT NULL,
   priority         INTEGER    NOT NULL,
   parent_flag      INTEGER    DEFAULT 0,
   status           INTEGER    DEFAULT 0,
   subscription_id  INTEGER,
   insert_time      INTEGER    NOT NULL,
   update_time      INTEGER    NOT NULL,
   PRIMARY KEY (id))
"""

class Elements(DBFormatter):
#class Elements(DASRESTFormatter): - fails
    sql = """SELECT id, wmtask_id, input_id, parent_queue_id, child_queue, num_jobs,
            priority, parent_flag, status, subscription_id, insert_time, update_time
            FROM wq_element"""

    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)
        
        print "Elements: results: '%s'" % results
        formResults = self.formatDict(results)
        print "Elements: form results: '%s'" % formResults
        return formResults
    


"""
T0/DAS/Database/Oracle/RunsByStates.py

self.addDAO('GET', 'runsbystatus', 'RunsByStates', args=['run_status'])

_RunsByStates_

Monitoring DAO classes for Runs in Tier0

__all__ = []
__revision__ = "$Id: Elements.py,v 1.1 2010/02/03 17:20:48 maxa Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class RunsByStates(DBFormatter):
    
    #TO: check what else is needed for return item
    sql = SELECT run.RUN_ID, run_status.STATUS, run.START_TIME 
               FROM run 
               INNER JOIN run_status ON run.RUN_STATUS = run_status.ID 
            WHERE run.RUN_STATUS = (SELECT ID FROM run_status WHERE
                             STATUS = :status)
    
    def execute(self, run_status, conn = None, transaction = False):

        _execute_

        Execute the SQL for the given status list and then format and return
        the result.

        if type(run_status) != list:
            run_status = [run_status]
                
        bindVars = []
        for status in run_status:
            bindVars.append({"status": status})
        
        result = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)
        
        return self.formatDict(result)
"""