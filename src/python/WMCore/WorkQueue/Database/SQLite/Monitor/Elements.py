"""
WMCore/WorkQueue/Database/SQLite/Monitor/Elements.py

DAO object for WorkQueue

WorkQueue database structure:
WMCore/WorkQueue/Database/CreateWorkQueueBase.py

hints on usage:
T0/DAS/Services/Tier0TomService.py
T0/DAS/Database/Oracle/RunsByStates.py

"""

__all__ = []




from WMCore.Database.DBFormatter import DBFormatter



class Elements(DBFormatter):
    sql = """SELECT id, wmtask_id, input_id, parent_queue_id, child_queue, num_jobs,
            priority, parent_flag, status, subscription_id, insert_time, update_time
            FROM wq_element"""

    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)
        
        formResults = self.formatDict(results)
        return formResults