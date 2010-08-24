"""
_Exists_

MySQL implementation of WMSpec.Exists
"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = """SELECT we.id FROM wq_element we
               INNER JOIN wq_wmtask wt ON wt.id = we.wmtask_id
               INNER JOIN wq_wmspec ws ON ws.id = wt.wmspec_id
               INNER JOIN wq_data wd ON wd.id = we.input_id
             WHERE ws.name = :spec_name AND wt.name = :task_name
                  AND wd.name = :data_name"""
    
    def format(self, result):
        result = DBFormatter.format(self, result)
        if len(result) == 0:
            return False
        else:
            return int(result[0][0])
            
    def execute(self, specName, taskName, dataName, 
                conn = None, transaction = False):
        binds ={"spec_name": specName, "task_name": 
                taskName, "data_name": dataName}
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)