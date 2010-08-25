"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for MySQL.

Inherit from CreateWMBSBase, and add MySQL specific substitutions (e.g. add 
INNODB) and specific creates (e.g. for time stamp and enum fields).
"""

__revision__ = "$Id: Create.py,v 1.1 2009/06/05 17:04:33 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.CreateWorkQueueBase import CreateWorkQueueBase

class Create(CreateWorkQueueBase):
    """
    Class to set up the WMBS schema in a MySQL database
    """
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """        
        CreateWorkQueueBase.__init__(self, logger, dbi)

        
    def execute(self, conn = None, transaction = None):
        for i in self.create.keys():
            self.create[i] = self.create[i] + " ENGINE=InnoDB"
            #self.create[i] = self.create[i].replace('INTEGER', 'INT(11)')
            
        return CreateWorkQueueBase.execute(self, conn, transaction)
        
