"""
Oracle implementation of AddLocation
"""
from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL):
    
    sql =  """insert into wmbs_location (id, se_name) 
              values (wmbs_location_SEQ.nextval, :location)"""
    
    def execute(self, sename = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(sename), 
                         conn = conn, transaction = transaction)
        return
        #return self.format(result)