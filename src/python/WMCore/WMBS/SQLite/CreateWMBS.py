"""
SQLite implementation of NewFileset
"""
from WMCore.WMBS.MySQL.CreateWMBS import CreateWMBS as CreateWMBSMySQL
from WMCore.WMBS.SQLite.Base import SQLiteBase

class CreateWMBS(CreateWMBSMySQL, SQLiteBase):
    
    def __init__(self, logger, dbinterface):
        CreateWMBSMySQL.__init__(self, logger, dbinterface)
        
        self.insert = {}
        
        self.create['wmbs_fileset'] = """CREATE TABLE wmbs_fileset (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name varchar(255) NOT NULL,
                open BOOLEAN NOT NULL DEFAULT FALSE,
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
                name         VARCHAR(255) NOT NULL,
                owner        VARCHAR(255))"""
        self.create['wmbs_subs_type'] = """CREATE TABLE wmbs_subs_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name varchar(255) NOT NULL)"""
        for subtype in ('Processing', 'Merge', 'Job'):
            self.insert['wmbs_subs_type_%s' % subtype] = """insert into
                            wmbs_subs_type (name) values (%s)""" % subtype
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
                
    def execute(self, conn = None, transaction = False):
        CreateWMBSMySQL.execute(self, conn, transaction)
        
        # insert sqlite only values (i.e. enum's)
        keys = self.insert.keys()
        self.logger.debug( keys )
        self.dbi.processData(self.insert.values(), conn = conn, transaction = transaction)
        
        