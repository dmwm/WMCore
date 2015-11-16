"""
_ResultSet_

A class to read in a SQLAlchemy result proxy and hold the data, such that the
SQLAlchemy result sets (aka cursors) can be closed. Make this class look as much
like the SQLAlchemy class to minimise the impact of adding this class.
"""

import sqlalchemy
import operator

class ResultRow(object):
    """Class which immitates sqlalchemy result row object which is basically a dict
    which allows iteration and values access"""
    def __init__(self, row, sqlKeys):
        self._data = row
        self._keys = [k for k in sqlKeys if k in row.keys()]
    def keys(self):
        """Imitate dict keys method"""
        return self._keys
    def values(self):
        """Imitate dict values method"""
        return [self._data[k] for k in self._keys]
    def __getitem__(self, key):
        """Imitate dict getitem method, i.e. access object by bracket"""
        if isinstance(key, int):
            return self._data[self._keys[key]]
        if isinstance(key, basestring):
            return self._data.get(key, '')
    def __setitem__(self, key, value):
        """Imitate dict setitem method"""
        if  key not in self._keys:
            self._keys.append(key)
        self._data[key] = value
    def __delitem__(self, key):
        """Imitate dict delitem method"""
        try:
            self._keys.remove(key)
        except ValueError:
            pass
        del self._data[key]
    def update(self, rec):
        """Imitate dict update method"""
        self._keys += [k for k in rec.keys() if k not in self._keys]
        self._data.update(rec)
    def get(self, key, default=None):
        """Imitate dict get method"""
        return self._data.get(key, default)
    def items(self):
        """Imitate dict items method"""
        return [pair for pair in self.iteritems()]
    def iteritems(self):
        """Imitate dict iteritems method"""
        for key in self._keys:
            yield (key, self.get(key))
    def __len__(self):
        return len(self._data.values())
    def __str__(self):
        """Imitate dict str method"""
        return str(self._data)
    def __repr__(self):
        """Imitate dict repr method"""
        return repr(self._data)

    def _op(self, other, op):
        """Helper function to perform given operation over other part"""
        if isinstance(other, dict):
            return op(self._data, other)
        if other:
            return op(self._data, other._data)
        return op(self._data, other)

    def __lt__(self, other):
        """Comparison lt operator implementation"""
        return self._op(other, operator.lt)

    def __le__(self, other):
        """Comparison le operator implementation"""
        return self._op(other, operator.le)

    def __ge__(self, other):
        """Comparison ge operator implementation"""
        return self._op(other, operator.ge)

    def __gt__(self, other):
        """Comparison gt operator implementation"""
        return self._op(other, operator.gt)

    def __eq__(self, other):
        """Comparison eq operator implementation"""
        return self._op(other, operator.eq)

    def __ne__(self, other):
        """Comparison ne operator implementation"""
        return self._op(other, operator.ne)

class ResultSet:
    """
    Class to read SQLAlchemy result proxy objects and convert it into
    python data structure. The class should provide iterable interface
    and hold fetched keys.
    """
    def __init__(self, resultproxy=None):
        self.keys, self.data = parse(resultproxy)
        self.pos = 0 # current position of records to be retrieved

    def __len__(self):
        """Length method"""
        return len(self.data)

    def __iter__(self):
        """Iterator method"""
        return iter(self.data)

    def close(self):
        """Imitate close of resultproxy object"""
        return

    def fetchone(self):
        """Imitate fetchone method of resultproxy object"""
        if len(self.data) > 0:
            return self.data[0]
        else:
            return []

    def fetchall(self):
        """Imitate fetchall method of resultproxy object"""
        return self.data

    def fetchmany(self, size=None):
        """Imitate fetchmany method of resultproxy object"""
        if  size == None: # all data
            return self.data
        if self.pos == 0 and size > len(self.data): # we've been asked for all data
            return self.data
        if self.pos+size > len(self.data):
            raise StopIteration
        data = self.data[self.pos:self.pos+size]
        self.pos = self.pos+size
        return data

    def add(self, resultproxy):
        """Add new resultproxy object into result set"""
        keys, data = parse(resultproxy)
        if self.keys:
            if keys != self.keys:
                msg = "WARNING, ResultRow:add, result proxy keys mismatch, %s=!%s" % (keys, self.keys)
                print(msg)
                self.keys = [k for k in keys if k not in self.keys]
        else:
            self.keys = keys
        for rec in data:
            self.data.append(rec)

def parse(resultproxy):
    """Helper function to parse SQLAlchemy resultproxy object"""
    values = []
    keys = []
    if resultproxy and not resultproxy.closed:
        data = []
        try:
            data = resultproxy.fetchall()
        except sqlalchemy.exc.ResourceClosedError:
            pass
        except Exception as exp:
            msg = 'ResultSet, failed to fetch resultproxy object, %s type=%s' \
                    % (str(exp), type(exp))
            print(msg) # need to get something when run within threads
            raise Exception(msg)
        for r in data:
            if  not keys:
                keys = [i.lower() for i in r.keys()]
            rec = ResultRow(dict(zip(keys, r.values())), keys)
            values.append(rec)
        resultproxy.close()
    return keys, values
