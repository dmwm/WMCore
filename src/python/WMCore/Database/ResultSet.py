"""
_ResultSet_

A class to read in a SQLAlchemy result proxy and hold the data, such that the 
SQLAlchemy result sets (aka cursors) can be closed. Make this class look as much
like the SQLAlchemy class to minimise the impact of adding this class.
"""
__revision__ = "$Id: ResultSet.py,v 1.1 2009/03/31 09:43:01 metson Exp $"
__version__ = "$Revision: 1.1 $"

class ResultSet:
    def __init__(self):
        self.data = []
        self.keys = []
        
    def fetchone(self):
        return self.data[0]
    
    def fetchall(self):
        return self.data
    
    def add(self, resultproxy):
        for r in resultproxy:
            if len(self.keys) == 0:
                self.keys = r.keys
            self.data.append(r)