"""
_AddTaskMySQL_

Oracle implementation of WMSpec.AddTaskMySQL
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WMSpec.AddTask import AddTask \
     as AddTaskMySQL
     
class AddTask(AddTaskMySQL):
    sql = """INSERT INTO wq_wmtask (wmspec_id, name, type, dbs_url) 
              SELECT (SELECT id FROM wq_wmspec WHERE name = :wmspec_name), 
                 :name, :type, :dbs_url FROM DUAL 
                 WHERE NOT EXISTS
                       (SELECT * FROM wq_wmtask 
                         WHERE wmspec_id = (SELECT id FROM wq_wmspec WHERE name = :wmspec_name)
                              AND name = :name) 
          """