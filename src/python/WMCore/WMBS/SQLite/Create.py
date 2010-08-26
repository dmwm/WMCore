"""
_Create_

Implementation of Create for SQLite.

Inherit from CreateWMBSBase, and add SQLite specific creates to the dictionary 
at some high value.
"""

__revision__ = "$Id: Create.py,v 1.6 2010/02/26 21:18:21 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

class Create(CreateWMBSBase):
    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateWMBSBase.__init__(self, logger, dbi, params)
        
    def execute(self, conn = None, transaction = None):
        for i in self.create.keys():
            self.create[i] = self.create[i].replace('AUTO_INCREMENT', 'AUTOINCREMENT')
            
        return CreateWMBSBase.execute(self, conn, transaction)
