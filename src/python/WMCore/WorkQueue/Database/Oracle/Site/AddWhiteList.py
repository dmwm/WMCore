"""
_AddWhiteList_

MySQL implementation of Site.AddWhiteList
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.AddWhiteList import AddWhiteList \
     as AddWhiteListMySQL

class AddWhiteList(AddWhiteListMySQL):
    
    sql = """INSERT INTO wq_element_site_validation (element_id, site_id, valid) 
             SELECT :element_id, (SELECT id FROM wq_site WHERE name = :site_name), 1 FROM DUAL
                  WHERE NOT EXISTS
                       (SELECT * FROM wq_element_site_validation WHERE element_id = :element_id
                       AND site_id = (SELECT id FROM wq_site WHERE name = :site_name))"""
             