#!/usr/bin/env python
"""
_SiteDBAPI_

API for retrieving information from SiteDB

"""
from __future__ import print_function
from __future__ import division
import json
import logging
from WMCore.Services.Service import Service

def unflattenJSON(data):
    """Tranform input to unflatten JSON format"""
    columns = data['desc']['columns']
    return [row2dict(columns, row) for row in data['result']]

def row2dict(columns, row):
    """Convert rows to dictionaries with column keys from description"""
    robj = {}
    for k, v in zip(columns, row):
        robj.setdefault(k, v)
    return robj



class SiteDBAPI(Service):
    """
    Class to define just the data interaction layer with SiteDB
    """

    def __init__(self, config={}, logger=None):
        config = dict(config)
        config.setdefault('endpoint', "https://cmsweb.cern.ch/sitedb/data/prod/")
        config.setdefault('logger', logging.getLogger())
        Service.__init__(self, config)

    def getJSON(self, callname, filename='result.json', clearCache=False, verb='GET', data={}):
        """
        _getJSON_

        retrieve JSON formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """

        result = ''
        if clearCache:
            self.clearCache(cachefile=filename, inputdata=data, verb=verb)
        try:
            # Set content_type and accept_type to application/json to get json returned from siteDB.
            # Default is text/html which will return xml instead
            # Add accept-encoding to gzip,identity to overwrite httplib default gzip,deflate,
            # which is not working properly with cmsweb
            f = self.refreshCache(cachefile=filename, url=callname, inputdata=data,
                                  verb=verb, contentType='application/json',
                                  incoming_headers={'Accept': 'application/json',
                                                    'accept-encoding': 'gzip,identity'})
            result = f.read()
            f.close()
        except IOError:
            raise RuntimeError("URL not available: %s" % callname)
        try:
            results = json.loads(result)
            results = unflattenJSON(results)
            return results
        except SyntaxError:
            self.clearCache(filename, inputdata=data, verb=verb)
            raise SyntaxError("Problem parsing data. Cachefile cleared. Retrying may work")
