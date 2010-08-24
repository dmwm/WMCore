"""
SQLite implementation of site.UpdateDataSiteMapping
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.UpdateDataSiteMapping \
    import UpdateDataSiteMapping as UpdateDataSiteMappingMySQL

class UpdateDataSiteMapping(UpdateDataSiteMappingMySQL):
    #TODO need to improve the query
    # duplicate insert can happen when two different workspec using the same block
    insertSQL = """INSERT INTO wq_data_site_assoc (data_id, site_id)
                        SELECT 
                            (SELECT id FROM wq_data WHERE name = :data),
                            (SELECT id from wq_site WHERE name = :site)
                        FROM DUAL 
                        WHERE NOT EXISTS
                            (SELECT * FROM wq_data_site_assoc
                             WHERE data_id = (SELECT id FROM wq_data WHERE name = :data)
                              AND
                                site_id = (SELECT id from wq_site WHERE name = :site))"""
