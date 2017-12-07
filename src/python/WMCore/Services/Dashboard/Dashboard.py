#!/usr/bin/env python
"""
_Dashboard_

Service class to be used for fetching site status and metrics
"""

import json
import logging

from WMCore.Services.Service import Service


class Dashboard(Service):
    """
    SSB provides a service that gives site status and metrics
    for CPU and IOBound slots
    """

    def __init__(self, url, logger=None):
        params = {}
        params['endpoint'] = url
        params['cacheduration'] = 0
        params['accept_type'] = 'application/json'
        params['content_type'] = 'application/json'
        params['method'] = 'GET'
        params['logger'] = logger if logger else logging.getLogger()

        Service.__init__(self, params)

    def getMetric(self, metricNumber):
        """
        Fetch one of the metrics maintained in SSB
        :param metricNumber: a number corresponding to the SSB metric
        :return: a dictionary
        """
        metricFile = "ssb_metric_%s.csv" % metricNumber
        metricUrl = '/request.py/getplotdata?columnid=%s&batch=1&lastdata=1' % metricNumber

        self['logger'].debug('Fetching data from %s, url %s' % (metricFile, metricUrl))
        results = self.refreshCache(metricFile, metricUrl)
        results = results.read()

        return json.loads(results).get('csvdata', {})
