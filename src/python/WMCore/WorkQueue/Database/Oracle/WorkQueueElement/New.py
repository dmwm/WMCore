"""
_New_

MySQL implementation of WorkQueueElement.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/11/20 23:00:01 sryu Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.New import New \
     as NewMySQL
     
class New(NewMySQL):
    sql = NewMySQL.sql
    sql_no_input = NewMySQL.sql_no_input
    #TODO:
    # this is not the thread safe way: not sure safe for the race condition even in one transaction
    # Need to define some unique values for the table other than id
    sql_get_id = """SELECT wq_element_SEQ.currval FROM DUAL"""
    
