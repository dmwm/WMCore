"""
_AddTaskMySQL_

Oracle implementation of WMSpec.AddTaskMySQL
"""

__all__ = []
__revision__ = "$Id: AddTask.py,v 1.2 2010/02/08 19:05:44 sryu Exp $"
__version__ = "$Revision: 1.2 $"

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