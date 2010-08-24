"""
Oracle implementation of AddLocation
"""
from WMCore.WMBS.MySQL.Locations.New import New as NewLocationMySQL

class New(NewLocationMySQL):
    
    sql =  """insert into wmbs_location (id, se_name) 
              values (wmbs_location_SEQ.nextval, :location)"""
              