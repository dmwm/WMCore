"""
MySQL implementation of Site.UpdateBlockSiteMapping
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class UpdateDataSiteMapping(DBFormatter):
    #TODO: How do this in one statement
    deleteSql = """DELETE FROM wq_data_site_assoc WHERE data_id =
                  (SELECT id FROM wq_data WHERE name = :data)"""

    # duplicate insert can happen when two different workspec using the same block
    insertSQL = """INSERT IGNORE INTO wq_data_site_assoc (data_id, site_id)
                        VALUES(
                            (SELECT id FROM wq_data WHERE name = :data),
                            (SELECT id from wq_site WHERE name = :site))"""


    def execute(self, dataSiteAssoc, fullResync,
                conn = None, transaction = False):

        data = [{'data' : x} for x in dataSiteAssoc.keys()]
        binds = []
        for item, sites in dataSiteAssoc.iteritems():
            binds.extend([{"data" : item, "site" : site} for site in sites])

        # only delete existing mappings if we are doing a full sync
        if fullResync:
            self.dbi.processData(self.deleteSql, data, conn = conn,
                             transaction = transaction)
        if binds:
            self.dbi.processData(self.insertSQL, binds, conn = conn,
                                 transaction = transaction)
        return
