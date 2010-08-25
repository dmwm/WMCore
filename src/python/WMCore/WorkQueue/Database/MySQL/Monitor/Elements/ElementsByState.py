"""
WMCore/WorkQueue/Database/MySQL/Monitor/ElementsByState.py

DAO object for WorkQueue.

WorkQueue database structure:
WMCore/WorkQueue/Database/CreateWorkQueueBase.py

hints on usage:
T0/DAS/Services/Tier0TomService.py
T0/DAS/Database/Oracle/RunsByStates.py

"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States


class ElementsByState(DBFormatter):
    sql = """SELECT id, wmtask_id, input_id, parent_queue_id, child_queue, num_jobs,
            priority, parent_flag, status, subscription_id, insert_time, update_time
            FROM wq_element WHERE status = :status"""

    def execute(self, status, conn = None, transaction = False):
        if type(status) != list:
            status = [status]
                
        bindVars = []
        m = "Incorrect input - unknown WorkQueue element state '%s'"
        for st in status:
            # assumes that the input argument status has been validated
            # but check anyway
            assert States.has_key(st), m % st
            s = States[st]
            bindVars.append({"status": s})
                        
        results = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)
                
        formResults = self.formatDict(results)
        return formResults