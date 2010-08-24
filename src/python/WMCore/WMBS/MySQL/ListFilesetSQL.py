from WMCore.WMBS.MySQL.Base import MySQLBase

class ListFileset(MySQLBase):
    sql = "select * from wmbs_fileset order by last_update, name"