#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
JSON wrapper around different JSON python implementation.
So far we use simplejson (json) and cjson, other modules can be
added in addition.
"""

__revision__ = "$Id: __init__.py,v 1.3 2010/01/10 16:36:32 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"
__all__ = ['loads', 'dumps']

_module = "json"

try:
    import cjson
    _module = "cjson"
except:
    pass
try:
    import json # python 2.6 and later
except:
    import simplejson as json # python 2.5 and earlier

def loads(idict, **kwargs):
    """
    Based on default _module invoke appropriate JSON decoding API call
    """
    if  _module == 'json':
        return json.loads(idict, **kwargs)
    elif _module == 'cjson':
        return cjson.decode(idict)

def load(source):
    """
    Use json.load for back-ward compatibility, since cjson doesn't
    provide this method. The load method works on file-descriptor
    objects.
    """
    return json.load(source)

def dumps(idict, **kwargs):
    """
    Based on default _module invoke appropriate JSON encoding API call
    """
    if  _module == 'json':
        return json.dumps(idict, **kwargs)
    elif _module == 'cjson':
        return cjson.encode(idict)

def dump(doc, source):
    """
    Use json.dump for back-ward compatibility, since cjson doesn't
    provide this method. The dump method works on file-descriptor
    objects.
    """
    return json.dump(doc, source)


class JSONEncoder(object):
    def __init__(self):
        self.encoder = json.JSONEncoder()

    def encode(self, idict):
        if  _module == 'cjson':
            return cjson.encode(idict)
        return self.encoder.encode(idict)

    def iterencode(self, idict):
        return self.encoder.iterencode(idict)

class JSONDecoder(object):
    def __init__(self):
        self.decoder = json.JSONDecoder()

    def decode(self, idict):
        if  _module == 'cjson':
            return cjson.decode(idict)
        return self.decoder.decode(idict)

    def raw_decode(self, idict):
        return self.decoder.raw_decode(idict)
