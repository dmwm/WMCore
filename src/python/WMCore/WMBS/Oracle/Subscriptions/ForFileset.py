"""
Oracle implementation of Files.InFileset
"""

from WMCore.WMBS.MySQL.Subscriptions.ForFileset import ForFileset as ForFilesetMySQL

class InFileset(ForFilesetMySQL):
    sql = ForFilesetMySQL.sql