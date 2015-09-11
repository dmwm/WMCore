"""
_ResultSet_

A class to read in a SQLAlchemy result proxy and hold the data, such that the
SQLAlchemy result sets (aka cursors) can be closed. Make this class look as much
like the SQLAlchemy class to minimise the impact of adding this class.
"""
class ResultSet:
    def __init__(self):
        self.data = []
        self.keys = []

    def close(self):
        return

    def fetchone(self):
        if len(self.data) > 0:
            return self.data[0]
        else:
            return []

    def fetchall(self):
        return self.data

    def add(self, resultproxy):
        if resultproxy.closed:
            return
        else:
            for r in resultproxy:
                if len(self.keys) == 0:
                    self.keys.extend(r.keys())
                self.data.append(r)

        return
