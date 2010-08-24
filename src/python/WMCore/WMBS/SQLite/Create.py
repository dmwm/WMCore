"""
_Create_

Implementation of Create for SQLite.

Inherit from CreateWMBSBase, and add SQLite specific creates to the dictionary 
at some high value.
"""

__revision__ = "$Id: Create.py,v 1.3 2008/12/11 08:55:30 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

class Create(CreateWMBSBase):
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateWMBSBase.__init__(self, logger, dbi)
        self.requiredTables.append('30wmbs_subs_type')
        
        self.create["30wmbs_subs_type"] = \
          """CREATE TABLE wmbs_subs_type (
             id   INTEGER      PRIMARY KEY AUTOINCREMENT,
             name VARCHAR(255) NOT NULL)"""

        for subType in ("Processing", "Merge", "Job"):
            subTypeQuery = "INSERT INTO wmbs_subs_type (name) values ('%s')" % \
                           subType
            self.inserts["wmbs_subs_type_%s" % subType] = subTypeQuery
            
    def execute(self, conn = None, transaction = None):
        for i in self.create.keys():
            self.create[i] = self.create[i].replace('AUTO_INCREMENT', 'AUTOINCREMENT')
            
        return CreateWMBSBase.execute(self, conn, transaction)
