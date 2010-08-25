"""
_ResultSet_

A class to read in a SQLAlchemy result proxy and hold the data, such that the 
SQLAlchemy result sets (aka cursors) can be closed. Make this class look as much
like the SQLAlchemy class to minimise the impact of adding this class.
"""

__revision__ = "$Id: ResultSet.py,v 1.5 2009/05/12 16:38:46 swakef Exp $"
__version__ = "$Revision: 1.5 $"

class ResultSet:
    def __init__(self):
        self.data = []
        self.keys = []
        self.rowcount = 0
        # DO NOT use with Oracle, provide backwards compatibility for prodagent
        self.lastrowid = None

    def close(self):
        return
    
    def fetchone(self):
        return self.data[0]
    
    def fetchall(self):
        return self.data
    
    def add(self, resultproxy):
        if resultproxy.closed:
            return
        
        for r in resultproxy:
            if len(self.keys) == 0:
                self.keys.extend(r.keys())
            self.data.append(r)

        self.lastrowid = resultproxy.lastrowid
        self.rowcount = len(self.data)
        
        return
