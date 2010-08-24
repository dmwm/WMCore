#!/usr/bin/env python
"""
_WMBSSQLLite_

SQLite specific implementations

"""
__revision__ = "$Id: SQLite.py,v 1.6 2008/05/02 13:49:43 metson Exp $"
__version__ = "$Revision: 1.6 $"
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
    def __init__(self, logger, engine):
        MySQLDialect.__init__(self, logger, engine)
        self.logger.info ("Instantiating SQLite WMBS object")
        self.create['wmbs_fileset'] = """CREATE TABLE wmbs_fileset (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name varchar(255) NOT NULL,
                last_update timestamp NOT NULL,
                UNIQUE (name))"""
        self.create['wmbs_fileset_files'] = """CREATE TABLE wmbs_fileset_files (
                file    int(11)      NOT NULL,
                fileset int(11) NOT NULL,
                insert_time timestamp NOT NULL,
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
        self.create['wmbs_workflow'] = """CREATE TABLE wmbs_workflow (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                spec         VARCHAR(255) NOT NULL,
                owner        VARCHAR(255))"""
        self.create['wmbs_subscription'] = """CREATE TABLE wmbs_subscription (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fileset INT(11) NOT NULL,
                workflow INT(11) NOT NULL,
                type    INT(11) NOT NULL,
                last_update timestamp NOT NULL,
                FOREIGN KEY(fileset) REFERENCES wmbs_fileset(id)
                    ON DELETE CASCADE
                FOREIGN KEY(type) REFERENCES wmbs_subs_type(id)
                    ON DELETE CASCADE
                FOREIGN KEY(workflow) REFERENCES wmbs_workflow(id)
                    ON DELETE CASCADE)""" 
        self.create['wmbs_job'] = """CREATE TABLE wmbs_job (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription INT(11) NOT NULL,
                last_update timestamp NOT NULL,
                FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                ON DELETE CASCADE)"""    
        
        self.insert['fileset'] = """insert into wmbs_fileset 
            (name, last_update) values (:fileset, :timestamp)"""
        self.insert['fileforfileset'] = """insert into wmbs_fileset_files 
            (file, fileset, insert_time) 
            values ((select id from wmbs_file_details where lfn = :file),
            (select id from wmbs_fileset where name = :fileset), :timestamp)"""
        self.insert['newsubscription'] = """insert into wmbs_subscription 
            (fileset, workflow, type, last_update) 
            values ((select id from wmbs_fileset where name =:fileset), 
            (select id from wmbs_workflow where spec = :spec and owner = :owner),
            (select id from wmbs_subs_type where name = :type), :timestamp)"""
            
            
        self.select['nextfiles'] = """
select file from wmbs_fileset_files where
fileset = (select fileset from wmbs_subscription where id=:subscription)
and file not in 
    (select file from wmbs_sub_files_acquired where subscription=:subscription)
and file not in 
    (select file from wmbs_sub_files_failed where subscription=:subscription)
and file not in 
    (select file from wmbs_sub_files_complete where subscription=:subscription)
"""
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
        
        binds = [{'type':'merge'}, {'type':'processing'}]
        self.processData("insert into wmbs_subs_type (name) values (:type)",
                         binds)
        
        MySQLDialect.createSubscriptionsTable(self)
             
    def insertFileset(self, fileset = None, conn = None, transaction = False):
        """
        insert a (list of) fileset(s) to WMBS
        """
        binds = None
        if type(fileset) == type('string'):
            binds = {'fileset':fileset, 'timestamp':self.timestamp()}
        elif type(fileset) == type([]):
            binds = []
            for f in fileset:
                binds.append({'fileset':f, 'timestamp':self.timestamp()})
        if binds:
            self.processData(self.insert['fileset'], binds)

    def insertFilesForFileset(self, files=None, size=0, events=0, \
                              run=0, lumi=0, fileset=None, \
                              conn = None, transaction = False):
        """
        Add the file to wmbs_file_details
        """ 
        self.logger.debug ("inserting %s for fileset %s" % (files, fileset))
        self.insertFiles(files, size, events, run, lumi, conn, transaction)
        binds = None
        #Now add the files into the mapping table
        if type(files) == type('string'):            
            binds = {'file': files, 
                     'fileset':fileset, 
                     'timestamp':self.timestamp()}
            
        elif type(files) == type([]):
            binds = []
            for f in files:
                binds.append({'file': f[0], 
                              'fileset':fileset, 
                              'timestamp':self.timestamp()})
                
        # Replace with some bulk operation
        self.processData(self.insert['fileforfileset'], binds, 
                         conn = conn, transaction = transaction)
                      
    def newSubscription(self, fileset = None, spec = None, owner = None,
                        subtype='processing', conn = None, transaction = False):
        """
        Make a new subscription
        """ 
        if type('string') == type(fileset):
            binds = {'fileset': fileset, 
                     'spec': spec, 
                     'owner': owner,
                     'type': subtype, 
                     'timestamp':self.timestamp()}
            self.processData(self.insert['newsubscription'], binds)
        elif type(fileset) == type([]):
            binds = []
            for f in fileset:
                binds.append({'fileset': f, 
                              'spec': spec, 
                               'owner': owner,
                              'type': subtype, 
                              'timestamp':self.timestamp()})
            self.processData(self.insert['newsubscription'], binds)             
            
