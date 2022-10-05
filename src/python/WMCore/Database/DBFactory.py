
from builtins import object
import threading

from sqlalchemy import create_engine
from sqlalchemy import __version__ as sqlalchemy_version
from WMCore.Database.Dialects import MySQLDialect
from WMCore.Database.Dialects import OracleDialect

class DBFactory(object):

    # class variable
    _engineMap = {}
    _defaultEngineParams = {"convert_unicode" : True,
                            "pool_recycle": 7200}

    def __init__(self, logger, dburl=None, options={}):
        self.logger = logger
        # get the engine parameter from option
        if 'engine_parameters' in options:
            self._defaultEngineParams.update(options['engine_parameters'])
            del options['engine_parameters']

        if dburl:
            self.dburl = dburl
        else:
            #This will be deprecated.
            """
            Need to make the dburl here. Possible formats are:

            mysql://host/database
            mysql://username@host/database
            mysql://username:password@host:port/database

            oracle://username:password@tnsName
            oracle://username:password@host:port/sidname

            """
            hostjoin = ''
            if 'dialect' in options:
                self.dburl = '%s://' % options['dialect']
                del options['dialect']
            if 'user' in options:
                self.dburl = '%s%s' % (self.dburl, options['user'])
                hostjoin='@'
                del options['user']
            if 'username' in options:
                self.dburl = '%s%s' % (self.dburl, options['username'])
                hostjoin='@'
                del options['username']
            if 'passwd' in options:
                self.dburl = '%s:%s' % (self.dburl, options['passwd'])
                del options['passwd']
            if 'password' in options:
                self.dburl = '%s:%s' % (self.dburl, options['password'])
                del options['password']
            if 'tnsName' in options:
                self.dburl = '%s%s%s' % (self.dburl, hostjoin, options['tnsName'])
                del options['tnsName']
            elif 'host' in options:
                self.dburl = '%s%s%s' % (self.dburl, hostjoin, options['host'])
                del options['host']
                if 'port' in options:
                    self.dburl = '%s:%s' % (self.dburl, options['port'])
                    del options['port']
            if 'host' in options:
                self.dburl = '%s/%s' % (self.dburl, options['host'])
                del options['host']
            if 'database' in options:
                self.dburl = '%s/%s' % (self.dburl, options['database'])
                del options['database']
            elif 'sid' in options:
                self.dburl = '%s/%s' % (self.dburl, options['sid'])
                del options['sid']

        if self.dburl.split(':')[0].lower() == "http":

            self.engine = None
            self.dia = None

        else:
            self.engine = self._engineMap.setdefault(self.dburl,
                                                     create_engine(self.dburl,
                                                                   connect_args=options,
                                                                   **self._defaultEngineParams)
                                                     )
            self.dia = self.engine.dialect

        self.lock = threading.Condition()


    def connect(self):
        self.lock.acquire()

        if self.engine:

            self.logger.debug("Using SQLAlchemy v.%s" % sqlalchemy_version)

            if isinstance(self.dia, MySQLDialect):
                from WMCore.Database.MySQLCore import MySQLInterface as DBInterface
            else:
                from WMCore.Database.DBCore import DBInterface
            # we instantiate within the lock so we can safely return the local instance.
            dbInterface =  DBInterface(self.logger, self.engine)

        else:
            dbInterface =  None

        self.lock.release()
        return dbInterface
