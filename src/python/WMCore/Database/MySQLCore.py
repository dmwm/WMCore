#!/usr/bin/env python
"""
_MySQLCore_

Handle bind variable parsing for MySQL.
"""

import copy

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.ResultSet import ResultSet

def bindVarCompare(a):
    """
    _bindVarCompare_

    Bind variables are represented as a tuple with the first element being the
    variable name and the second being it's position in the query.  We sort on
    the position in the query.
    """
    return a[1]

def stringLengthCompare(a):
    """
    _stringLengthCompare_

    Sort comparison function to sort strings by length.
    Since we want to sort from longest to shortest, this must be reversed when used
    """
    return len(a)


class MySQLInterface(DBInterface):
    def substitute(self, origSQL, origBindsList):
        """
        _substitute_

        Transform as set of bind variables from a list of dictionaries to a list
        of tuples:

        b = [ {'bind1':'value1a', 'bind2': 'value2a'},
        {'bind1':'value1b', 'bind2': 'value2b'} ]

        Will be transformed into:

        b = [ ('value1a', 'value2a'), ('value1b', 'value2b')]

        Don't need to substitute in the binds as executemany does that
        internally. But the sql will also need to be reformatted, such that
        :bind_name becomes %s.

        See: http://www.devshed.com/c/a/Python/MySQL-Connectivity-With-Python/5/
        """
        if origBindsList == None:
            return origSQL, None

        origBindsList = self.makelist(origBindsList)
        origBind = origBindsList[0]

        bindVarPositionList = []
        updatedSQL = copy.copy(origSQL)

        # We process bind variables from longest to shortest to avoid a shorter
        # bind variable matching a longer one.  For example if we have two bind
        # variables: RELEASE_VERSION and RELEASE_VERSION_ID the former will
        # match against the latter, causing problems.  We'll sort the variable
        # names by length to guard against this.
        bindVarNames = list(origBind)
        bindVarNames.sort(key=stringLengthCompare, reverse=True)

        bindPositions = {}
        for bindName in bindVarNames:
            searchPosition = 0

            while True:
                bindPosition = origSQL.lower().find(":%s" % bindName.lower(),
                                                    searchPosition)
                if bindPosition == -1:
                    break

                if bindPosition not in bindPositions:
                    bindPositions[bindPosition] = 0
                    bindVarPositionList.append((bindName, bindPosition))
                searchPosition = bindPosition + 1

            searchPosition = 0
            while True:
                bindPosition = updatedSQL.lower().find(":%s" % bindName.lower(),
                                                       searchPosition)

                if bindPosition == -1:
                    break

                left = updatedSQL[0:bindPosition]
                right = updatedSQL[bindPosition + len(bindName) + 1:]
                updatedSQL = left + "%s" + right

        bindVarPositionList.sort(key=bindVarCompare)

        mySQLBindVarsList = []
        for origBind in origBindsList:
            mySQLBindVars = []
            for bindVarPosition in bindVarPositionList:
                mySQLBindVars.append(origBind[bindVarPosition[0]])

            mySQLBindVarsList.append(tuple(mySQLBindVars))

        return (updatedSQL, mySQLBindVarsList)

    def executebinds(self, s = None, b = None, connection = None,
                     returnCursor = False):
        """
        _executebinds_

        Execute a SQL statement that has a single set of bind variables.
        Transform the bind variables into the format that MySQL expects.
        """
        s, b = self.substitute(s, b)
        return DBInterface.executebinds(self, s, b, connection, returnCursor)

    def executemanybinds(self, s = None, b = None, connection = None,
                         returnCursor = False):
        """
        _executemanybinds_

        Execute a SQL statement that has multiple sets of bind variables.
        Transform the bind variables into the format that MySQL expects.
        """
        newsql, binds = self.substitute(s, b)

        return DBInterface.executemanybinds(self, newsql, binds, connection,
                                            returnCursor)
