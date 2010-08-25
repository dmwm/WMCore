
import threading

from sqlalchemy import create_engine
from sqlalchemy import __version__ as sqlalchemy_version
from WMCore.Database.Dialects import MySQLDialect
from WMCore.Database.Dialects import SQLiteDialect
from WMCore.Database.Dialects import OracleDialect

class DBFactory(object):
    
    # class variable
    engineMap = {}
    
    def __init__(self, logger, dburl=None, options={}):
        self.logger = logger
        if dburl:
            self.dburl = dburl
        else:
            """
            Need to make the dburl here. Possible formats are:
            
            postgres://username:password@host:port/database
            
            sqlite:////absolute/path/to/database.txt
            sqlite:///relative/path/to/database.txt
            sqlite://
            sqlite://:memory:
            
            mysql://host/database
            mysql://username@host/database
            mysql://username:password@host:port/database
            
            oracle://username:password@tnsName
            oracle://username:password@host:port/sidname

            """
            hostjoin = ''
            if 'dialect' in options.keys():
                self.dburl = '%s://' % options['dialect']
                del options['dialect']
            if 'user' in options.keys():
                self.dburl = '%s%s' % (self.dburl, options['user'])
                hostjoin='@'
                del options['user']
            if 'username' in options.keys():
                self.dburl = '%s%s' % (self.dburl, options['username'])
                hostjoin='@'
                del options['username']
            if 'passwd' in options.keys():
                self.dburl = '%s:%s' % (self.dburl, options['passwd'])
                del options['passwd']
            if 'password' in options.keys():
                self.dburl = '%s:%s' % (self.dburl, options['password'])
                del options['password']
            if 'tnsName' in options.keys():
                self.dburl = '%s%s%s' % (self.dburl, hostjoin, options['tnsName'])
                del options['tnsName']
            elif 'host' in options.keys():
                self.dburl = '%s%s%s' % (self.dburl, hostjoin, options['host'])
                del options['host']
                if 'port' in options.keys():
                    self.dburl = '%s:%s' % (self.dburl, options['port'])
                    del options['port']
            if 'host' in options.keys():
                self.dburl = '%s/%s' % (self.dburl, options['host'])
                del options['host']
            if 'database' in options.keys():
                self.dburl = '%s/%s' % (self.dburl, options['database'])
                del options['database']
            elif 'sid' in options.keys():
                self.dburl = '%s/%s' % (self.dburl, options['sid'])
                del options['sid']    
                
           
        self.engine = self.engineMap.setdefault(self.dburl,     
                                         create_engine(self.dburl, 
                                                       #echo_pool=True,
                                                       convert_unicode=True, 
                                                       encoding='utf-8',
                                                       strategy='threadlocal',
                                                       pool_size = 25,
                                                       connect_args = options)
                                                  )
        self.dia = self.engine.dialect
        self.lock = threading.Condition()

        
    def connect(self):
        self.lock.acquire()
        self.logger.debug("Using SQLAlchemy v.%s" % sqlalchemy_version)
                
        if isinstance(self.dia, MySQLDialect):
            from WMCore.Database.MySQLCore import MySQLInterface as DBInterface
        else:
            from WMCore.Database.DBCore import DBInterface
        # we instantiate within the lock so we can safely return the local instance.
        dbInterface =  DBInterface(self.logger, self.engine)
        self.lock.release()
        return dbInterface
