#!/usr/bin/env python
"""
_Core_

Core Database APIs


"""
import sys
from copy import copy

from Utils.IteratorTools import grouper
import WMCore.WMLogging
from WMCore.DataStructs.WMObject import WMObject
from WMCore.Database.ResultSet import ResultSet

class DBInterface(WMObject):
    """
    Base class for doing SQL operations using a SQLAlchemy engine, or
    pre-exisitng connection.

    processData will take a (list of) sql statements and a (list of)
    bind variable dictionaries and run the statements on the DB. If
    necessary it will substitute binds into the sql (MySQL).

    TODO:
        Add in some suitable exceptions in one or two places
        Test the hell out of it
        Support executemany()
    """

    logger = None
    engine = None

    def __init__(self, logger, engine):
        self.logger = logger
        self.logger.info ("Instantiating base WM DBInterface")
        self.engine = engine
        self.maxBindsPerQuery = 500

    def buildbinds(self, sequence, thename, therest=[{}]):
        """
        Build a list of binds. Can be used recursively, e.g.:
        buildbinds(file, 'file', buildbinds(pnn, 'location'), {'lumi':123})
        TODO: replace with an appropriate map function
        """
        binds = []
        for r in sequence:
            for i in self.makelist(therest):
                thebind = copy(i)
                thebind[thename] = r
                binds.append(thebind)
        return binds

    def executebinds(self, s=None, b=None, connection=None,
                     returnCursor=False):
        """
        _executebinds_

        returns a list of sqlalchemy.engine.base.ResultProxy objects
        """
        if b == None:
            resultProxy = connection.execute(s)
        else:
            resultProxy = connection.execute(s, b)

        if returnCursor:
            return resultProxy

        result = ResultSet()
        result.add(resultProxy)
        resultProxy.close()
        return result

    def executemanybinds(self, s=None, b=None, connection=None,
                         returnCursor=False):
        """
        _executemanybinds_
        b is a list of dictionaries for the binds, e.g.:

        b = [ {'bind1':'value1a', 'bind2': 'value2a'},
        {'bind1':'value1b', 'bind2': 'value2b'} ]

        see: http://www.gingerandjohn.com/archives/2004/02/26/cx_oracle-executemany-example/

        Can't executemany() selects - so do each combination of binds here instead.
        This will return a list of sqlalchemy.engine.base.ResultProxy object's
        one for each set of binds.

        returns a list of sqlalchemy.engine.base.ResultProxy objects
        """

        s = s.strip()
        if s.lower().startswith('select', 0, 6):
            ### selecting many
            if returnCursor:
                result = []
                for bind in b:
                    result.append(connection.execute(s, bind))
            else:
                result = ResultSet()
                for bind in b:
                    resultproxy = connection.execute(s, bind)
                    result.add(resultproxy)
                    resultproxy.close()
        else:
            ### inserting / updating many
            result = ResultSet()
            for sliceBinds in grouper(b, self.maxBindsPerQuery):
                res = connection.execute(s, sliceBinds)
                result.add(res)

        return self.makelist(result)

    def connection(self):
        """
        Return a connection to the engine (from the connection pool)
        """
        return self.engine.connect()


    def processData(self, sqlstmt, binds=None, conn=None, transaction=False,
                    returnCursor=False, encodeType="ascii"):
        """
        set conn if you already have an active connection to reuse
        set transaction = True if you already have an active transaction

        """
        binds = binds or []
        connection = None
        try:
            if not conn:
                connection = self.connection()
            else:
                connection = conn

            result = []
            # Can take either a single statement or a list of statements and binds
            sqlstmt = self.makelist(sqlstmt)
            binds = self.makelist(binds)
            binds = self.convertBinds(binds, encodeType)
            if sqlstmt and not binds:
                # Should only be run by create statements
                if not transaction:
                    #WMCore.WMLogging.sqldebug("transaction created in DBInterface")
                    trans = connection.begin()

                for i in sqlstmt:
                    r = self.executebinds(i, connection=connection,
                                          returnCursor=returnCursor)
                    result.append(r)

                if not transaction:
                    trans.commit()
            elif len(binds) > len(sqlstmt) and len(sqlstmt) == 1:
                #Run single SQL statement for a list of binds - use execute_many()
                if not transaction:
                    trans = connection.begin()

                result.extend(self.executemanybinds(sqlstmt[0], binds, connection=connection,
                                                    returnCursor=returnCursor))

                if not transaction:
                    trans.commit()
            elif len(binds) == len(sqlstmt):
                # Alan: do we actually have this use case?!?!
                # Run a list of SQL for a list of binds
                if not transaction:
                    trans = connection.begin()

                for i, s in enumerate(sqlstmt):
                    b = binds[i]

                    r = self.executebinds(s, b, connection=connection,
                                          returnCursor=returnCursor)
                    result.append(r)

                if not transaction:
                    trans.commit()
            else:
                self.logger.exception(
                    "DBInterface.processData Nothing executed, problem with your arguments")
                self.logger.exception(
                    "DBInterface.processData SQL = %s" % sqlstmt)
                WMCore.WMLogging.sqldebug('DBInterface.processData  sql is %s items long' % len(sqlstmt))
                WMCore.WMLogging.sqldebug('DBInterface.processData  binds are %s items long' % len(binds))
                assert_value = False
                if len(binds) == len(sqlstmt):
                    assert_value = True
                WMCore.WMLogging.sqldebug('DBInterface.processData are binds and sql same length? : %s' % (assert_value))
                WMCore.WMLogging.sqldebug('sql: %s\n binds: %s\n, connection:%s\n, transaction:%s\n' %
                                           (sqlstmt, binds, connection, transaction))
                WMCore.WMLogging.sqldebug('type check:\nsql: %s\n binds: %s\n, connection:%s\n, transaction:%s\n' %
                                           (type(sqlstmt), type(binds), type(connection), type(transaction)))
                raise Exception("""DBInterface.processData Nothing executed, problem with your arguments
                Probably mismatched sizes for sql (%i) and binds (%i)""" % (len(sqlstmt), len(binds)))
        finally:
            if not conn and connection != None:
                connection.close() # Return connection to the pool
        return result

    def convertBinds(self, bindsList, encodeType):
        """
        Convert any unicode string key/value to its correspondant byte string.
        Let an exception be raised in case the string cannot be encoded in ascii.
        :param bindsList: list of dictionaries corresponding to the binds
        :return: updates are made in-place, but a reference is returned anyways
        """
        if sys.version_info.major == 3:
            self.logger.debug("Then there isn't anything to be done")
            return bindsList

        for item in bindsList:
            for k in item:
                ### TODO: this will have to be re-thought for python3 migration
                # there is no `unicode` object type
                if isinstance(k, unicode):
                    item[k.encode(encodeType)] = item.pop(k)
                if isinstance(item[k], unicode):
                    item[k] = item[k].encode(encodeType)
        return bindsList
