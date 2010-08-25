from WMCore.Database.DBCore import DBInterface
from WMCore.Database.Dialects import MySQLDialect

def bindVarCompare(a, b):
    """
    _bindVarCompare_

    Bind variables are represented as a tuple with the first element being the
    variable name and the second being it's position in the query.  We sort on
    the position in the query.
    """
    if a[1] > b[1]:
        return 1
    elif a[1] == b[1]:
        return 0
    else:
        return -1

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
        for bindName in origBind.keys():
            searchPosition = 0

            while True:
                bindPosition = origSQL.find(":%s" % bindName, searchPosition)
                if bindPosition == -1:
                    break

                bindVarPositionList.append((bindName, bindPosition))
                searchPosition = bindPosition + 1

            origSQL = origSQL.replace(":%s" % bindName, "%s")

        bindVarPositionList.sort(bindVarCompare)

        mySQLBindVarsList = []
        for origBind in origBindsList:
            mySQLBindVars = []
            for bindVarPosition in bindVarPositionList:
                mySQLBindVars.append(origBind[bindVarPosition[0]])

            mySQLBindVarsList.append(tuple(mySQLBindVars))

        return (origSQL, mySQLBindVarsList)

    def executebinds(self, s = None, b = None, connection = None):
        """
        _executebinds_

        Execute a SQL statement that has a single set of bind variables.
        Transform the bind variables into the format that MySQL expects.
        """
        s, b = self.substitute(s, b)
        return DBInterface.executebinds(self, s, b, connection)
    
    def executemanybinds(self, s = None, b = None, connection = None):
        """
        _executemanybinds_

        Execute a SQL statement that has multiple sets of bind variables.
        Transform the bind variables into the format that MySQL expects.        
        """
        newsql, bind_list = self.substitute(s, b)

        if newsql.lower().endswith('select', 0, 6):
            """
            Trying to select many
            """
            result = []
            for bind in bind_list:
                result.append(connection.execute(newsql, bind))
            return self.makelist(result)

        result = connection.execute(newsql, bind_list)
        return self.makelist(result)
