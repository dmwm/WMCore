"""
_CreateWMBS_

Implementation of CreateWMBS for SQLite.

Inherit from CreateWMBSBase, and add SQLite specific creates to the dictionary 
at some high value.
"""

__revision__ = "$Id: CreateWMBS.py,v 1.15 2008/10/03 11:31:20 metson Exp $"
__version__ = "$Reivison: $"

from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

class CreateWMBS(CreateWMBSBase):
    def __init__(self, logger, dbInterface):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateWMBSBase.__init__(self, logger, dbInterface)
        self.requiredTables.append('30wmbs_subs_type')
        
        self.create["30wmbs_subs_type"] = \
          """CREATE TABLE wmbs_subs_type (
             id   INTEGER      PRIMARY KEY AUTOINCREMENT,
             name VARCHAR(255) NOT NULL)"""

        for subType in ("Processing", "Merge", "Job"):
            subTypeQuery = "INSERT INTO wmbs_subs_type (name) values ('%s')" % \
                           subType
            self.inserts["wmbs_subs_type_%s" % subType] = subTypeQuery
