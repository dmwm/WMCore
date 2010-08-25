"""
MySQL implementation of Site.CheckValidity
"""

__all__ = []
__revision__ = "$Id: CheckValidity.py,v 1.1 2009/11/20 23:00:00 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class CheckValidity(DBFormatter):
    
    #This query is prerry ugly: find the better way to handle that.
    sql = """SELECT (SELECT count(*) FROM wq_element_site_validation
                      WHERE element_id = :elementID AND valid = 1) white_site_flag,
                    (SELECT count(*) FROM wq_element_site_validation
                      INNER JOIN wq_site ON (site_id = wq_site.id)
                     WHERE element_id = :elementID AND wq_site.name = :siteName
                           AND valid = 1) white_site,
                    (SELECT count(*) FROM wq_element_site_validation
                      INNER JOIN wq_site ON (site_id = wq_site.id)
                     WHERE element_id = :elementID AND wq_site.name = :siteName
                           AND valid = 0) black_site
                FROM DUAL"""

    def execute(self, elementID, siteName,
                conn = None, transaction = False):

        binds = {'elementID': elementID, 'siteName': siteName}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        formattedResult = self.formatDict(result)
        
        if formattedResult[0]['black_site'] > 0:
            return False
        
        if formattedResult[0]['white_site'] > 0:
            return True
        elif formattedResult[0]['white_site_flag'] > 0: 
            return False
        else:
            return True
        