"""
WMCore/WorkQueue/Database/SQLite/Monitor/ElementsById.py

DAO object for WorkQueue.

WorkQueue database structure:
WMCore/WorkQueue/Database/CreateWorkQueueBase.py

hints on usage:
T0/DAS/Services/Tier0TomService.py
T0/DAS/Database/Oracle/RunsByStates.py

"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter


class ElementsById(DBFormatter):
    sql = """SELECT id, wmtask_id, input_id, parent_queue_id, child_queue, num_jobs,
            priority, parent_flag, status, subscription_id, insert_time, update_time
            FROM wq_element WHERE id = :id"""

    def execute(self, id, conn = None, transaction = False):
        if type(id) != list:
            id = [id]
                
        bindVars = [{"id": i} for i in id]
                
        results = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)
                
        formResults = self.formatDict(results)
        return formResults