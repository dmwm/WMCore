from sqlalchemy import create_engine
from sqlalchemy import __version__ as sqlalchemy_version
from WMCore.Database.Dialects import MySQLDialect
from WMCore.Database.Dialects import SQLiteDialect
from WMCore.Database.Dialects import OracleDialect

class DBFactory(object):
    def __init__(self, logger, dburl):
        self.logger = logger
        self.dburl = dburl
        
    def connect(self):
        self.logger.debug("Using SQLAlchemy v.%s" % sqlalchemy_version)
        self.logger.debug("creating DB engine %s" % self.dburl)
        engine = create_engine(self.dburl, convert_unicode=True, encoding='utf-8', pool_size=10, pool_recycle=30)

        dia = engine.dialect
        if isinstance(dia, MySQLDialect):
            from WMCore.Database.MySQLCore import MySQLInterface as DBInterface
        else:
            from WMCore.Database.DBCore import DBInterface
            
        return DBInterface(self.logger, engine)