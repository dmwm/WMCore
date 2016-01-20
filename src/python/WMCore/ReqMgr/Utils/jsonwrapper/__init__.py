#!/usr/bin/env python
# -*- coding: ISO-8859-1 -*-

"""
JSON wrapper around different JSON python implementations.
We use simplejson (json), cjson and yajl JSON implementation.

NOTE: different JSON implementation handle floats in different way
Here are few examples


..doctest::

    r1={"ts":time.time()}
    print r1
    {'ts': 1374255843.891289}

Python json:

..doctest::

    print json.dumps(r1), json.loads(json.dumps(r1))
    {"ts": 1374255843.891289} {u'ts': 1374255843.891289}

"""

__author__ = "Valentin Kuznetsov <vkuznet@gmail.com>"

import json


def loads(idict, **kwargs):
    """
    Based on default MODULE invoke appropriate JSON decoding API call
    """
    return json.loads(idict, **kwargs)


def load(source):
    """
    Use json.load for back-ward compatibility, since cjson doesn't
    provide this method. The load method works on file-descriptor
    objects.
    """
    return json.load(source)


def dumps(idict, **kwargs):
    """
    Based on default MODULE invoke appropriate JSON encoding API call
    """
    return json.dumps(idict, **kwargs)


def dump(doc, source):
    """
    Use json.dump for back-ward compatibility, since cjson doesn't
    provide this method. The dump method works on file-descriptor
    objects.
    """
    return json.dump(doc, source)


class JSONEncoder(object):
    """
    JSONEncoder wrapper
    """

    def __init__(self, **kwargs):
        self.encoder = json.JSONEncoder(**kwargs)

    def encode(self, idict):
        """Decode JSON method"""
        return self.encoder.encode(idict)

    def iterencode(self, idict):
        "Encode input dict"
        return self.encoder.iterencode(idict)


class JSONDecoder(object):
    """
    JSONDecoder wrapper
    """

    def __init__(self, **kwargs):
        self.decoder = json.JSONDecoder(**kwargs)

    def decode(self, istring):
        """Decode JSON method"""
        return self.decoder.decode(istring)

    def raw_decode(self, istring):
        "Decode given string"
        return self.decoder.raw_decode(istring)
