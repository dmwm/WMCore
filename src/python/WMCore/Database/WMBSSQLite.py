#!/usr/bin/env python
"""
_WMBSSQLLite_

SQLite specific implementations

"""
__revision__ = "$Id: WMBSSQLite.py,v 1.1 2008/04/10 19:45:10 evansde Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.WMBSMySQL import wmbsMySQL

class wmbsSQLite(wmbsMySQL):
    """
    initial implementation of WMBS API for SQLite
         
    Changes the appropriate dictionary key for dialect specific SQL
    If necessary over ride the relevant methods for dialect specific operations
     e.g. creating sequences, indexes, timestamps.
    """
    def __init__(self, logger, connection):
        wmbsMySQL.__init__(self, logger, connection)
        self.logger.info ("Instantiating SQLite WMBS object")
        self.create['wmbs_fileset'] = """CREATE TABLE wmbs_fileset (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name varchar(255) NOT NULL,
                last_update timestamp NOT NULL,
                UNIQUE (name))"""
        self.create['wmbs_fileset_files'] = """CREATE TABLE wmbs_fileset_files (
                file    int(11)      NOT NULL,
                fileset int(11) NOT NULL,
                status int(11),
                FOREIGN KEY(fileset) references wmbs_fileset(id)
                FOREIGN KEY(status) references wmbs_file_status(id)
                ON DELETE CASCADE)"""
        self.create['wmbs_file_details'] = """CREATE TABLE wmbs_file_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lfn     VARCHAR(255) NOT NULL,
                size    int(11),
                events  int(11),
                run     int(11),
                lumi    int(11))"""
        self.create['wmbs_location'] = """CREATE TABLE wmbs_location (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                se_name VARCHAR(255) NOT NULL,
                UNIQUE(se_name))"""
        self.create['wmbs_subscription'] = """CREATE TABLE wmbs_subscription (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fileset INT(11) NOT NULL,
                type    INT(11) NOT NULL,
                last_update timestamp NOT NULL,
                FOREIGN KEY(fileset) REFERENCES wmbs_fileset(id)
                FOREIGN KEY(type) REFERENCES wmbs_subs_type(id)
                ON DELETE CASCADE)""" 
        self.create['wmbs_job'] = """CREATE TABLE wmbs_job (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription INT(11) NOT NULL,
                last_update timestamp NOT NULL,
                FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                ON DELETE CASCADE)"""    
        
        self.insert['fileset'] = "insert into wmbs_fileset (name, last_update) values (:fileset, :timestamp)"
        self.insert['newsubscription'] = """insert into wmbs_subscription (fileset, type, last_update) 
                values ((select id from wmbs_fileset where name =:fileset), 
                (select id from wmbs_subs_type where name = :type), :timestamp)"""
        self.select['nextfiles'] = """select file from wmbs_fileset_files where
            fileset = (select fileset from wmbs_subscription where id=:subscription)
            and file not in (select file from wmbs_sub_files_acquired where subscription=:subscription)
            and file not in (select file from wmbs_sub_files_failed where subscription=:subscription)
            and file not in (select file from wmbs_sub_files_complete where subscription=:subscription)"""

        self.select['subscriptionsoftype'] = """select id, fileset from wmbs_subscription 
                where type=(select id from wmbs_subs_type where name = :type)"""
        self.select['subscriptionsforfilesetoftype'] = """select id from wmbs_subscription 
                where type=(select id from wmbs_subs_type where name = :type) 
                and fileset=(select id from wmbs_fileset where name =:fileset)"""
                                     
    def createFileTable(self):
        sql = """CREATE TABLE wmbs_file_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name varchar(255) NOT NULL)"""        
        self.processData(sql)
        
        binds = [{'status':'active'}, {'status':'inactive'}, {'status':'invalid'}]
        self.processData("insert into wmbs_file_status (name) values (:status)", binds)
        wmbsMySQL.createFileTable(self)              

    def createFileDetailsTable(self):  
        wmbsMySQL.createFileDetailsTable(self)
        sql = "create index ix_wmbs_file_details_lfn on wmbs_file_details (lfn)"
        self.processData(sql)
        
    def createSubscriptionsTable(self):
        sql = """CREATE TABLE wmbs_subs_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name varchar(255) NOT NULL)"""  
        self.processData(sql)
        
        binds = [{'type':'merge'}, {'type':'processing'}]
        self.processData("insert into wmbs_subs_type (name) values (:type)", binds)
        wmbsMySQL.createSubscriptionsTable(self)
             
    def insertFileset(self, fileset = None):
        # insert a (list of) fileset(s) to WMBS
        import datetime
        import time
        t = datetime.datetime.now()
        timestamp = time.mktime(t.timetuple())
        binds = None
        if type(fileset) == type('string'):
            binds = {'fileset':fileset, 'timestamp':timestamp}
            self.processData(self.insert['fileset'], binds)
        elif type(fileset) == type([]):
            binds = []
            for f in fileset:
                binds.append({'fileset':f, 'timestamp':timestamp})
            self.logger.debug( binds )
            self.processData(self.insert['fileset'], binds)
            
    def newSubscription(self, fileset = None, subtype='processing'):
        import datetime
        import time
        t = datetime.datetime.now()
        timestamp = time.mktime(t.timetuple())
        if type('string') == type(fileset):
            binds = {'fileset': fileset, 'type': subtype, 'timestamp':timestamp}
            self.processData(self.insert['newsubscription'], binds)
        elif type(fileset) == type([]):
            binds = []
            for f in fileset:
                binds.append({'fileset': f, 'type': subtype, 'timestamp':timestamp})
            self.processData(self.insert['newsubscription'], binds)             
            
