"""
SQLite implementation of Files.InFileset
"""

from WMCore.WMBS.MySQL.Subscriptions.ForFileset import ForFileset as ForFilesetMySQL

class ForFileset(ForFilesetMySQL):
    sql = ForFilesetMySQL.sql
