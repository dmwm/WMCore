"""
_AddBlackList_

Oracle implementation of Site.AddBlackList
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Site.AddBlackList import AddBlackList \
     as AddBlackListMySQL

class AddBlackList(AddBlackListMySQL):

    sql = """INSERT INTO wq_element_site_validation (element_id, site_id, valid) 
             SELECT :element_id, (SELECT id FROM wq_site WHERE name = :site_name), 0 FROM DUAL
                  WHERE NOT EXISTS
                       (SELECT * FROM wq_element_site_validation WHERE element_id = :element_id
                       AND site_id = (SELECT id FROM wq_site WHERE name = :site_name))"""
