"""
_DBFormatter_

A class to define the standardised formatting of database results.
Holds a bunch of helper methods to format input and output of sql
interactions.
"""



import datetime
import time

from WMCore.DataStructs.WMObject import WMObject

class DBFormatter(WMObject):
    def __init__(self, logger, dbinterface):
        """
        The class holds a connection to the database in self.dbi. This is a
        DBCore.DBInterface object.
        """
        self.logger = logger
        self.dbi = dbinterface

    def truefalse(self, value):
        if value in ('False', 'FALSE', 'n', 'N', 'NO', 'No'):
            value = 0
        return bool(value)

    def convertdatetime(self, t):
        return int(time.mktime(t.timetuple()))

    def timestamp(self):
        """
        generate a timestamp
        """
        t = datetime.datetime.now()
        return self.convertdatetime(t)

    def format(self, result):
        """
        Some standard formatting, put all records into a list
        """
        out = []
        for r in result:
            for i in r.fetchall():
                row = []
                for j in i:
                    row.append(j)
                out.append(row)

            r.close()

        return out

    def formatOne(self, result):
        """
        Return one record
        """
        out = []
        for r in result:
            if r.rowcount == 0:
                return []
            for i in r.fetchone():
                out.append(i)
        return out

    def formatDict(self, result):
        """
        Returns an array of dictionaries representing the results
        """
        dictOut = []
        for r in result:
            descriptions = r.keys
            for i in r.fetchall():
                #WARNING: this can generate errors for some stupid reason
                # in both oracle and mysql.
                entry = {}
                for index in xrange(0,len(descriptions)):
                    # WARNING: Oracle returns table names in CAP!
                    if type(i[index]) == unicode:
                        entry[str(descriptions[index].lower())] = str(i[index])
                    else:
                        entry[str(descriptions[index].lower())] = i[index]

                dictOut.append(entry)

            r.close()

        return dictOut

    def formatOneDict(self, result):
        """
        Return a dictionary representing the first record
        """
        if (len(result) == 0):
            return {}

        r = result[0]
        description = map(lambda x: str(x).lower(), r.keys)
        if len(r.data) < 1:
            return {}

        return dict(zip(description, r.fetchone()))


    def formatCursor(self, cursor, size=10):
        """
        Fetch the driver cursor directly.
        Tested only with cx_Oracle.
        Cursor must be already executed.
        Use fetchmany(size = default arraysize = 50)

        """
        keys = [x.lower() for x in cursor.keys]
        result = []
        rapp = result.append
        while True:
            if not cursor.closed :
                rows = cursor.fetchmany(size=size)
                if not rows:
                    cursor.close()
                    break
                for r in rows:
                    rapp(dict(zip(keys, r)))
            else: break
        if not cursor.closed:
            cursor.close()
        return result


    def getBinds(self, **kwargs):
        binds = {}
        for i in kwargs.keys():
            binds = self.dbi.buildbinds(self.dbi.makelist(kwargs[i]), i, binds)
        return binds

    def execute(self, conn = None, transaction = False, returnCursor = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(self.sql, self.getBinds(),
                         conn = conn, transaction = transaction,
                                      returnCursor = returnCursor)
        return self.format(result)

    def executeOne(self, conn = None, transaction = False, returnCursor = False):
        """
        A simple select with no binds/arguments is the default
        """
        result = self.dbi.processData(self.sql, self.getBinds(),
                         conn = conn, transaction = transaction,
                                      returnCursor = returnCursor)
        return self.formatOne(result)
