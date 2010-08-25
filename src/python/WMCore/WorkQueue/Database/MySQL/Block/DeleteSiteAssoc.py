"""
_New_

MySQL implementation of Block.DeleteSiteAssoc
"""

__all__ = []
__revision__ = "$Id: DeleteSiteAssoc.py,v 1.1 2009/06/15 20:56:57 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """DELELE wq_block_site_assoc 
                 WHERE block_id = 
                     (SLECT id FROM wq_block WHERE name = :blockName) 
          """

    def execute(self, blockName, conn = None, transaction = False):
        binds = {"blockName": blockName}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return