"""
_AddTaskMySQL_

SQLite implementation of WMSpec.AddTaskMySQL
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WMSpec.AddTask import AddTask \
     as AddTaskMySQL

class AddTask(AddTaskMySQL):
    
    sql = AddTaskMySQL.sql.replace('IGNORE', 'OR IGNORE')