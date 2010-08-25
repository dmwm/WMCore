"""
MySQL implementation of Site.UpdateBlockSiteMapping
"""

__all__ = []
__revision__ = "$Id: UpdateDataSiteMapping.py,v 1.1 2009/09/03 15:44:18 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class UpdateDataSiteMapping(DBFormatter):
    #TODO: How do this in one statement
    deleteSql = """DELETE FROM wq_data_site_assoc WHERE data_id =
                  (SELECT id FROM wq_data WHERE name = :data)"""

    insertSQL = """INSERT INTO wq_data_site_assoc (data_id, site_id)
                        VALUES(
                            (SELECT id FROM wq_data WHERE name = :data),
                            (SELECT id from wq_site WHERE name = :site))"""


    def execute(self, dataSiteAssoc, conn = None, transaction = False):

        data = [{'data' : x} for x in dataSiteAssoc.keys()]
        binds = []
        for item, sites in dataSiteAssoc.iteritems():
            binds.extend([{"data" : item, "site" : site} for site in sites])

        self.dbi.processData(self.deleteSql, data, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.insertSQL, binds, conn = conn,
                             transaction = transaction)
        return
