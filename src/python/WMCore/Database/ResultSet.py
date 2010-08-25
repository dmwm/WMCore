"""
_ResultSet_

A class to read in a SQLAlchemy result proxy and hold the data, such that the 
SQLAlchemy result sets (aka cursors) can be closed. Make this class look as much
like the SQLAlchemy class to minimise the impact of adding this class.
"""

__revision__ = "$Id: ResultSet.py,v 1.13 2010/05/04 21:37:29 sryu Exp $"
__version__ = "$Revision: 1.13 $"

import threading

class ResultSet:
    def __init__(self):
        self.data = []
        self.keys = []
        self.rowcount = 0
        #Oracle has no lastrowid, so this is only set if it exists.  See add()
        self.lastrowid = None

    def close(self):
        return

    def fetchone(self):
        return self.data[0]

    def fetchall(self):
        return self.data

    def add(self, resultproxy):
        myThread = threading.currentThread()
        #Has to be there to provide some Oracle functionality
        #Oracle resultproxy doesn't have lastrowid
        if hasattr(resultproxy, 'lastrowid'):
            self.lastrowid = resultproxy.lastrowid

        if not hasattr(myThread, "dialect"):
            # By default, we'll use MySQL
            myThread.dialect = "MySQL"
        
        self.rowcount += resultproxy.rowcount
        
        if resultproxy.closed:
            return
        else:
            for r in resultproxy:
                if len(self.keys) == 0:
                    self.keys.extend(r.keys())
                self.data.append(r)
            # rowcount only increase when resultproxy is 
            # iterated for select in Oracle.
            if myThread.dialect == "Oracle":
                self.rowcount += resultproxy.rowcount
        
        return
