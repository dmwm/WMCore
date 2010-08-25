"""
_AddTaskMySQL_

Oracle implementation of WMSpec.AddTaskMySQL
"""

__all__ = []
__revision__ = "$Id: AddTask.py,v 1.1 2009/11/20 23:00:00 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WMSpec.AddTask import AddTask \
     as AddTaskMySQL
     
class AddTask(AddTaskMySQL):
    sql = """INSERT INTO wq_wmtask (wmspec_id, name, dbs_url) 
              SELECT (SELECT id FROM wq_wmspec WHERE name = :wmspec_name), 
                 :name, :dbs_url FROM DUAL 
                 WHERE NOT EXISTS
                       (SELECT * FROM wq_wmtask 
                         WHERE wmspec_id = (SELECT id FROM wq_wmspec WHERE name = :wmspec_name)
                              AND name = :name) 
          """