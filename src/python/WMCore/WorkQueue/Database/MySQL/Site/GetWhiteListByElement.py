"""
MySQL implementation of Site.GetWhiteListByElement
"""

__all__ = []
__revision__ = "$Id: GetWhiteListByElement.py,v 1.1 2009/11/20 23:00:00 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetWhiteListByElement(DBFormatter):
    
    #This query is prerry ugly: find the better way to handle that.
    sql = """SELECT ws.name 
               FROM wq_element_site_validation wsv
               INNER JOIN wq_site ws ON (ws.id = wsv.site_id)
             WHERE wsv.element_id = :element_id AND wsv.valid = 1
          """

    def execute(self, elementID, conn = None, transaction = False):

        binds = {'element_id': elementID}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        results = self.format(results)
        siteList = []
        for r in results:
            siteList.append(r[0])
        return siteList