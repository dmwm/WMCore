#!/usr/bin/env python
"""
_WMBSSQLLite_

SQLite specific implementations

"""
__revision__ = "$Id: SQLite.py,v 1.8 2008/06/10 17:38:21 metson Exp $"
__version__ = "$Revision: 1.8 $"
import datetime
import time
from WMCore.WMBS.MySQL import MySQLDialect

class SQLiteDialect(MySQLDialect):
    """
    initial implementation of WMBS API for SQLite
         
    Changes the appropriate dictionary key for dialect specific SQL
    If necessary over ride the relevant methods for dialect specific operations
     e.g. creating sequences, indexes, timestamps.
    """
    sqlIgnoreError = ''#'or ignore'
    
    def __init__(self, logger, engine):
        print "THIS CLASS IS DEPRECATED!!"
        
        self.insert['newlocation'] = """
            insert %s into wmbs_location (se_name) values (:location) 
        """ % self.sqlIgnoreError
#        self.insert['fileset'] = """insert %s into wmbs_fileset 
#            (name, last_update) values (:fileset, :timestamp)""" % self.sqlIgnoreError
        self.insert['newsubscription'] = """insert %s into wmbs_subscription 
            (fileset, workflow, type, parentage, last_update) 
            values ((select id from wmbs_fileset where name =:fileset), 
            (select id from wmbs_workflow where spec = :spec and owner = :owner),
            (select id from wmbs_subs_type where name = :type), :parentage, :timestamp)""" % '' #self.sqlIgnoreError
        self.insert['newfile'] = """
            insert %s into wmbs_file_details (lfn, size, events, run, lumi) 
            values (:lfn, :size, :events, :run, :lumi)""" % "" #self.sqlIgnoreError
        #        self.select['nextfiles'] = """
                
        #select file from wmbs_fileset_files where
        #fileset = (select fileset from wmbs_subscription where id=:subscription)
        #and file not in 
        #    (select file from wmbs_sub_files_acquired where subscription=:subscription)
        #and file not in 
        #    (select file from wmbs_sub_files_failed where subscription=:subscription)
        #and file not in 
        #    (select file from wmbs_sub_files_complete where subscription=:subscription)
        #"""
        self.select['idofsubscription'] = """
            select id from wmbs_subscription where type=(select id from wmbs_subs_type where name = :type) 
                and workflow = (select id from wmbs_workflow where spec = :spec and owner = :owner)
                and fileset=(select id from wmbs_fileset where name =:fileset)
        """

        self.select['subscriptionsoftype'] = """
select id, fileset, workflow from wmbs_subscription 
where type=(select id from wmbs_subs_type where name = :type)"""
        self.select['subscriptionsforfilesetoftype'] = """
select id, workflow from wmbs_subscription 
where type=(select id from wmbs_subs_type where name = :type) 
and fileset=(select id from wmbs_fileset where name =:fileset)"""
        self.select['subscriptionsforworkflowoftype'] = """
            select id from wmbs_subscription where workflow=:name and
            type=(select id from wmbs_subs_type where name = :type)
        """
        
    def timestamp(self):
        """
        generate a timestamp
        """
        t = datetime.datetime.now()
        return time.mktime(t.timetuple())
                                            
    def createFileTable(self):
        """
        Create the wmbs_file_status table for SQLite
        """
        sql = """CREATE TABLE wmbs_file_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name varchar(255) NOT NULL)"""        
        self.processData(sql)
        
        binds = [{'status':'active'}, 
                 {'status':'inactive'}, 
                 {'status':'invalid'}]
        
        self.processData("""
            insert into wmbs_file_status (name) values (:status)""", 
            binds)
        
        MySQLDialect.createFileTable(self)              

    def createFileDetailsTable(self): 
        """
        Create wmbs_file_details_lfn table and associated index
        """ 
        MySQLDialect.createFileDetailsTable(self)
        sql = "create index ix_wmbs_file_details_lfn on wmbs_file_details (lfn)"
        self.processData(sql)
        
    def createSubscriptionsTable(self):
        """
        Create subscription tabel and ancillary subscription type table for SQLite
        """
        sql = """CREATE TABLE wmbs_subs_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name varchar(255) NOT NULL)"""  
        self.processData(sql)
        
        binds = [{'type':'Merge'}, {'type':'Processing'}, {'type':'Job'}]
        self.processData("insert into wmbs_subs_type (name) values (:type)",
                         binds)
        
        MySQLDialect.createSubscriptionsTable(self)
             
