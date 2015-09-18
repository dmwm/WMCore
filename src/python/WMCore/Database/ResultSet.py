"""
_ResultSet_

A class to read in a SQLAlchemy result proxy and hold the data, such that the
SQLAlchemy result sets (aka cursors) can be closed. Make this class look as much
like the SQLAlchemy class to minimise the impact of adding this class.
"""

import sqlalchemy

class ResultSet:
    """
    Class to read SQLAlchemy result proxy objects and convert it into
    python data structure. The class should provide iterable interface
    and hold fetched keys.
    """
    def __init__(self, resultproxy):
        self.data = [] # values of query
        self.keys = set()
        if not resultproxy.closed:
            data = []
            try:
                data = resultproxy.fetchall()
            except sqlalchemy.exc.ResourceClosedError:
                pass
            except Exception as exp:
                msg = 'ResultSet, failed to fetch resultproxy object, %s type=%s' \
                        % (str(exp), type(exp))
                raise Exception(msg)
            for r in data:
                if  not self.keys:
                    self.keys = r.keys()
                self.data.append(r.values())
            resultproxy.close()

    def __len__(self):
        "Length method"
        return len(self.data)

    def __iter__(self):
        "Iterator method"
        return iter(self.data)

    def close(self):
        "Immitate close of resultproxy object"
        return

    def fetchone(self):
        "Immitate fetchone method of resultproxy object"
        if len(self.data) > 0:
            return self.data[0]
        else:
            return []

    def fetchall(self):
        "Immitate fetchmany method of resultproxy object"
        return self.data

### Valentin, I left this class as an example
### how to convert resultproxy set directly into stream of dictionaries
### So far this job is delegated to DBFormatter formatDict/formatDictOne
### methods, but it must be done earlier at DB level.
class ResultSetExampleDictConverter:
    def __init__(self, resultproxy):
        self.data = []
        self.keys = set()
        if not resultproxy.closed:
            data = []
            try:
                data = resultproxy.fetchall()
            except sqlalchemy.exc.ResourceClosedError:
                pass
            except Exception as exp:
                msg = 'ResultSet, failed to fetch resultproxy object, %s type=%s' \
                        % (str(exp), type(exp))
                raise Exception(msg)
            for r in data:
                if  not self.keys:
                    self.keys = r.keys()
                rec = {}
                for pair in r.items():
                    # convert each pair into dict, a pair comes from db
                    # as (key, value). We also lower key names since ORACLE has capitalize them
                    rec.update({pair[0].lower(): pair[1]})
                self.data.append(rec)
            resultproxy.close()

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def close(self):
        return

    def fetchone(self):
        if len(self.data) > 0:
            return self.data[0]
        else:
            return []

    def fetchall(self):
        return self.data
