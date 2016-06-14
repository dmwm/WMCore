#!/usr/bin/env python
# -*- coding: ISO-8859-1 -*-

"""
JSON wrapper around different JSON python implementation.
So far we use simplejson (json) and cjson, other modules can be
added in addition.
"""

import json


def loads(idict, **kwargs):
    """
    Based on default _module invoke appropriate JSON decoding API call
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
    Based on default _module invoke appropriate JSON encoding API call
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
    def __init__(self):
        self.encoder = json.JSONEncoder()

    def encode(self, idict):
        return self.encoder.encode(idict)

    def iterencode(self, idict):
        return self.encoder.iterencode(idict)


class JSONDecoder(object):
    def __init__(self):
        self.decoder = json.JSONDecoder()

    def decode(self, idict):
        return self.decoder.decode(idict)

    def raw_decode(self, idict):
        return self.decoder.raw_decode(idict)
