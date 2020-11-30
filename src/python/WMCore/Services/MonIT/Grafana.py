#!/usr/bin/env python
# coding=utf-8
"""
Wrapper class, based on the PyCuRL module, providing an interface
to CERN MonIT Grafana APIs
"""
from __future__ import division, print_function, absolute_import
from builtins import str, object
from future import standard_library
standard_library.install_aliases()

import logging
import json
from copy import copy
from pprint import pformat
from urllib.parse import urljoin

from WMCore.Services.pycurl_manager import RequestHandler

class Grafana(object):
    """
    Service class providing functionality to CERN monitoring Grafana APIs
    """

    def __init__(self, token, configDict=None):
        """
        Constructs a Grafana object.
        :param token: mandatory string with the key/token string
        :param configDict: dictionary with extra parameters, such as:
          logger: logger object
          endpoint: string with the url/endpoint to be used
          cacheduration: float with the cache duration, in hours
          headers: dictionary with the headers to be used
        """
        self._token = token
        self.configDict = configDict or {}
        self.configDict.setdefault('endpoint', "https://monit-grafana.cern.ch")
        self.configDict.setdefault('cacheduration', 0)  # in hours
        if 'headers' not in self.configDict:
            self.configDict.setdefault('headers', {})
            self.configDict['headers'].update({"Accept": "application/json"})
            self.configDict['headers'].update({"Content-Type": "application/json"})

        self.logger = self.configDict.get("logger", logging.getLogger())
        self.logger.info("MonIT service initialized with parameters: %s", self.configDict)

    def updateToken(self, token):
        """
        Update token to be used in requests made through this module
        :param token: string with the new token
        """
        self._token = token

    def _postRequest(self, url, params, verb='POST', verbose=0):
        "Helper function to POST request to given URL"
        mgr = RequestHandler(logger=self.logger)
        headers = copy(self.configDict['headers'])
        headers.update({"Authorization": self._token})

        try:
            data = mgr.getdata(url, params, headers, verb=verb, verbose=verbose)
            return json.loads(data)
        except Exception as exc:
            self.logger.error("Failed to retrieve data from MonIT. Error: %s", str(exc))
            return None

    def getAPIData(self, apiName, queryStr=''):
        """
        _getAPIData_

        Retrieve data from a given Grafana API and index name
        :param apiName: string with the API name/number
        :param queryStr: string with the whole query logic
        :return: data as retrieved from Grafana
        """
        uri = urljoin(self.configDict['endpoint'], "/api/datasources/proxy/%s/_msearch" % apiName)
        return self._postRequest(uri, queryStr)

    def getSSBData(self, pathName, metricName, indexName="monit_prod_cmssst_*",
                   apiName="9475", queryStr=''):
        """
        _getSSBData_

        Retrieve data from the SSB index in Grafana
        :param pathName: string with the path to the correct metric, mapped from SSB as:
            columnid=237   ProdStatus   path="sts15min"  data.prod_status
            columnid=160   CPU bound    path="scap15min" data.core_cpu_intensive
            columnid=161   I/O bound    path="scap15min" data.core_io_intensive
            columnid=159   Prod Cores   path="scap15min" data.core_production
            columnid=136   Real Cores   path="scap15min" data.core_max_used
            columnid=107   Tape Pledge  path="scap15min" data.tape_pledge
        :param indexName: optional string with the grafana index name
        :param apiName: optional string with the Grafana API number
        :param queryStr: optional string with the full query logic
        :return: dictionary with the site names and the metric value
        """
        # TODO: allow flexible time range instead of the hardwired last 24h
        # TODO: allow flexible number of rows, instead of the hardwired 500
        results = {}
        if metricName not in ('prod_status', 'core_cpu_intensive', 'core_io_intensive',
                              'core_production', 'core_max_used', 'tape_pledge'):
            self.logger.error("SSB metric name '%s' is NOT supported.", metricName)
            return results

        uri = urljoin(self.configDict['endpoint'], "/api/datasources/proxy/%s/_msearch" % apiName)
        if not queryStr:
            ### NOTE: these '\n' new lines are mandatory to get it working...
            queryStr = '{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["%s"]}\n' % indexName
            queryStr += '{"size":500,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":"now-1d","lte":"now","format":"epoch_millis"}}},' \
                        '{"query_string":{"analyze_wildcard":true,' \
                        '"query":"metadata.type: ssbmetric AND metadata.type_prefix:raw AND metadata.path: %s"}}]}},' \
                        '"sort":{"metadata.timestamp":{"order":"desc","unmapped_type":"boolean"}},' \
                        '"script_fields":{},"docvalue_fields":["metadata.timestamp"]}\n' % pathName
        self.logger.info("Calling API: %s with query: %s", uri, pformat(queryStr))

        data = self._postRequest(uri, queryStr)
        #self.logger.debug("Data retrieved: %s", pformat(data))
        if data:
            # now parse the ugly output
            hits = data['responses'][0]['hits']['hits']
            for item in hits:
                site = item['_source']['data']['name']
                metricValue = item['_source']['data'][metricName]
                tStamp = int(item['sort'][0])

                results.setdefault(site, {})
                if tStamp > results[site].get("timeStamp", 0):
                    results[site][metricName] = metricValue
                    results[site]["timeStamp"] = tStamp

            data = results

        return data
