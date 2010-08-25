"""
_New_

MySQL implementation of Block.AddSiteAssoc
"""

__all__ = []
__revision__ = "$Id: AddSiteAssoc.py,v 1.1 2009/06/15 20:56:57 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT wq_block_site_assoc (block_id, site_id) 
                 VALUES (SLECT id FROM wq_block WHERE name = :blockName, 
                 SLECT id FROM wq_site WHERE name = :siteName)
          """

    def execute(self, blockName, siteName, conn = None, transaction = False):
        binds = {"blockName": blockName, "siteName": siteName}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return