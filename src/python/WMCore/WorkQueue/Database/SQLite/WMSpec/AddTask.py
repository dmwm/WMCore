"""
_AddTaskMySQL_

SQLite implementation of WMSpec.AddTaskMySQL
"""

__all__ = []
__revision__ = "$Id: AddTask.py,v 1.1 2009/11/20 22:59:59 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.WMSpec.AddTask import AddTask \
     as AddTaskMySQL

class AddTask(AddTaskMySQL):
    
    sql = AddTaskMySQL.sql.replace('IGNORE', 'OR IGNORE')