"""
SQLite implementation of site.UpdateBlockSiteMapping
"""

__all__ = []
__revision__ = "$Id: UpdateDataSiteMapping.py,v 1.2 2010/05/27 18:19:34 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.MySQL.Site.UpdateDataSiteMapping \
    import UpdateDataSiteMapping as UpdateDataSiteMappingMySQL

class UpdateDataSiteMapping(UpdateDataSiteMappingMySQL):
    deleteSql = UpdateDataSiteMappingMySQL.deleteSql.replace('IGNORE', 'OR IGNORE', 1)
    insertSQL = UpdateDataSiteMappingMySQL.insertSQL.replace('IGNORE', 'OR IGNORE', 1)
