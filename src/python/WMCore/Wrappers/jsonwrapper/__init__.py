#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
JSON wrapper around different JSON python implementation.
So far we use simplejson (json) and cjson, other modules can be
added in addition.
"""

__revision__ = "$Id: __init__.py,v 1.2 2009/12/16 14:22:15 valya Exp $"
__version__ = "$Revision: 1.2 $"
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
def dumps(idict, **kwargs):
    """
    Based on default _module invoke appropriate JSON encoding API call
    """
    if  _module == 'json':
        return json.dumps(idict, **kwargs)
    elif _module == 'cjson':
        return cjson.encode(idict)

