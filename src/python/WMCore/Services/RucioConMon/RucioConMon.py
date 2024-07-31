#!/usr/bin/env python
# coding=utf-8
"""
Rucio Consistency Monitor Service class developed on top of the native WMCore
Service APIs providing custom output and handling as necessary for the
CMS Workload Management system
"""

from __future__ import division, print_function, absolute_import
from future import standard_library

from urllib.parse import urlencode

import json
import logging

from WMCore.Services.Service import Service
from Utils.Utilities import decodeBytesToUnicode


standard_library.install_aliases()


class RucioConMon(Service):
    """
    API for dealing with retrieving information from Rucio Consistency Monitor
    """

    def __init__(self, url, logger=None, configDict=None):
        """
        Init method for the RucioConMon Class
        """
        configDict = configDict or {}
        configDict.setdefault('endpoint', url)
        configDict.setdefault('cacheduration', 1)  # in hours
        configDict.setdefault('accept_type', 'application/json')
        configDict.setdefault('content_type', 'application/json')
        configDict['logger'] = logger if logger else logging.getLogger()
        super(RucioConMon, self).__init__(configDict)
        self['logger'].debug("Initializing RucioConMon with url: %s", self['endpoint'])

    def _getResult(self, uri, callname="", clearCache=False, args=None):
        """
        Either fetch data from the cache file or query the data-service
        :param uri: The endpoint uri
        :param callname: alias for caller function
        :param clearCache: parameter to control the cache behavior
        :param args: additional parameters to HTTP request call
        :return:    A dictionary
        """

        # NOTE: Unlike the common case we are not using the callname for building
        #       apiUrl. we  are using it only for giving proper name of the cache
        #       file.
        # NOTE: The 'callname' should not contain '/', otherwise the cache is
        #       tried to be created with a a non existing subdirectory structure

        cachedApi = "%s.json" % callname
        # apiUrl = '%s?json&preset=%s' % (uri, callname)
        apiUrl = uri

        self['logger'].debug('Fetching data from %s, with args %s', apiUrl, args)
        if args:
            apiUrl = "%s&%s" % (apiUrl, urlencode(args, doseq=True))

        if clearCache:
            self.clearCache(cachedApi, args)
        with self.refreshCache(cachedApi, apiUrl, decoder=True, binary=False) as istream:
            return json.load(istream)

    def _getResultZipped(self, uri, callname="", clearCache=True):
        """
        This method retrieves gzipped content, instead of the standard json format.
        :param uri: The endpoint uri
        :param callname: alias for caller function
        :param clearCache: parameter to control the cache behavior
        :return: yields a single record from the data retrieved
        """
        cachedApi = callname
        if clearCache:
            self.clearCache(cachedApi)

        with self.refreshCache(cachedApi, uri, decoder=False, binary=True) as istream:
            for line in istream:
                line = decodeBytesToUnicode(line).replace("\n", "")
                yield line

    def getRSEStats(self):
        """
        Gets the latest statistics from the RucioConMon, together with the last
        update timestamps for all RSEs known to CMS Rucio
        :return: A dictionary
        """
        uri = "stats"
        rseStats = self._getResult(uri, callname='stats')
        return rseStats

    def getRSEUnmerged(self, rseName, zipped=False):
        """
        Gets the list of all unmerged files in an RSE
        :param rseName: The RSE whose list of unmerged files to be retrieved
        :param zipped:  If True the interface providing the zipped lists will be called
        :return: a generator of unmerged files for the RSE in question
        """
        # NOTE: The default API provided by Rucio Consistency Monitor is in a form of a
        #       zipped file/stream. Currently we are using the newly provided json API
        #       But in in case we figure out the data is too big we may need to
        #       implement the method with the zipped API and use disc cache for
        #       reading/streaming from file. This will prevent any set arithmetic
        #       in the future.
        if zipped:
            uri = "files?rse=%s&format=raw" % rseName
            callname = '{}.zipped'.format(rseName)
            rseUnmerged = self._getResultZipped(uri, callname=callname, clearCache=True)
        else:
            uri = "files?rse=%s&format=json" % rseName
            callname = '{}.json'.format(rseName)
            rseUnmerged = self._getResult(uri, callname=callname)
        # now lazily return items
        for item in rseUnmerged:
            yield item
