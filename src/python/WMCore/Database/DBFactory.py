from sqlalchemy import create_engine
from sqlalchemy import __version__ as sqlalchemy_version
from WMCore.Database.Dialects import MySQLDialect
from WMCore.Database.Dialects import SQLiteDialect
from WMCore.Database.Dialects import OracleDialect

class DBFactory(object):
    def __init__(self, logger, dburl, options={}):
        self.logger = logger
        if dburl:
            self.dburl = dburl
        else:
            if 'port' in options.keys():
                self.dburl = '%s://%s:%s@%s:%s' % (options['dialect'],
                                                   options['user'],
                                                   options['passwd'],
                                                   options['port'],
                                                   options['tnsName'])
                del options['dialect']
                del options['user']
                del options['passwd']
                del options['port']
                del options['tnsName']
            else:
                self.dburl = '%s://%s:%s@%s' % (options['dialect'],
                                                options['user'],
                                                options['passwd'],
                                                options['tnsName'])
                del options['dialect']
                del options['user']
                del options['passwd']
                del options['tnsName']
            
        self.engine = create_engine(self.dburl, 
                               #echo_pool=True,
                               convert_unicode=True, 
                               encoding='utf-8',
                               strategy='threadlocal',
                               connect_args = options)
        self.dia = self.engine.dialect
        
    def connect(self):
        self.logger.debug("Using SQLAlchemy v.%s" % sqlalchemy_version)
        self.logger.debug("creating DB engine %s" % self.dburl)
                
        if isinstance(self.dia, MySQLDialect):
            from WMCore.Database.MySQLCore import MySQLInterface as DBInterface
        else:
            from WMCore.Database.DBCore import DBInterface
            
        return DBInterface(self.logger, self.engine)
