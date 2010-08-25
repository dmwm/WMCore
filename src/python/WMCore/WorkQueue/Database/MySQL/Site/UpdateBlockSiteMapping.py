"""
MySQL implementation of Site.UpdateBlockSiteMapping
"""

__all__ = []
__revision__ = "$Id: UpdateBlockSiteMapping.py,v 1.1 2009/08/18 23:18:15 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class UpdateBlockSiteMapping(DBFormatter):
    #TODO: How do this in one statement
    deleteSql = """DELETE FROM wq_block_site_assoc WHERE block_id =
                  (SELECT id FROM wq_block WHERE name = :block)"""

    insertSQL = """INSERT INTO wq_block_site_assoc (block_id, site_id)
                        VALUES(
                            (SELECT id FROM wq_block WHERE name = :block),
                            (SELECT id from wq_site WHERE name = :site))"""


    def execute(self, blockSiteAssoc, conn = None, transaction = False):

        blocks = [{'block' : x} for x in blockSiteAssoc.keys()]
        binds = []
        for block, sites in blockSiteAssoc.iteritems():
            binds.extend([{"block" : block, "site" : site} for site in sites])

        self.dbi.processData(self.deleteSql, blocks, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.insertSQL, binds, conn = conn,
                             transaction = transaction)
        return
