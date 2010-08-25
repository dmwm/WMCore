"""
SQLite implementation of site.UpdateDataSiteMapping
"""

__all__ = []
__revision__ = "$Id: UpdateDataSiteMapping.py,v 1.2 2010/03/29 21:12:14 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.Site.UpdateDataSiteMapping \
    import UpdateDataSiteMapping as UpdateDataSiteMappingMySQL

class UpdateDataSiteMapping(UpdateDataSiteMappingMySQL):
    insertSQL = """INSERT INTO wq_data_site_assoc (data_id, site_id)
                        VALUES(
                            (SELECT id FROM wq_data WHERE name = :data),
                            (SELECT id from wq_site WHERE name = :site))"""
