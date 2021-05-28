from __future__ import (division, print_function)

from builtins import zip, str, bytes

from future import standard_library
standard_library.install_aliases()

import json
import logging
import re
from urllib.parse import urlencode

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


class CRIC(Service):
    """
    Class which provides client APIs to the CRIC service.
    """

    def __init__(self, url=None, logger=None, configDict=None):
        """
        configDict is a dictionary with parameters that are passed
        to the super class
        """
        url = url or "https://cms-cric.cern.ch/"
        configDict = configDict or {}
        configDict.setdefault('endpoint', url)
        configDict.setdefault('cacheduration', 1)  # in hours
        configDict.setdefault('accept_type', 'application/json')
        configDict.setdefault('content_type', 'application/json')
        configDict['logger'] = logger if logger else logging.getLogger()
        super(CRIC, self).__init__(configDict)
        self['logger'].debug("Initializing CRIC with url: %s", self['endpoint'])

    def _getResult(self, uri, callname="", args=None, unflatJson=True):
        """
        Either fetch data from the cache file or query the data-service
        :param metricNumber: a number corresponding to the SSB metric
        :return: a dictionary
        """
        cachedApi = "%s.json" % callname
        apiUrl = '%s?json&preset=%s' % (uri, callname)

        self['logger'].debug('Fetching data from %s, with args %s', apiUrl, args)
        # need to make our own encoding, otherwise Requests class screws it up
        if args:
            apiUrl = "%s&%s" % (apiUrl, urlencode(args, doseq=True))

        data = self.refreshCache(cachedApi, apiUrl)
        results = data.read()
        data.close()

        results = json.loads(results)
        if unflatJson:
            results = unflattenJSON(results)
        return results

    def _CRICUserQuery(self, callname, unflatJson=True):
        """
        :param callname: name of the call
        :return: dict of the result
        """

        uri = "/api/accounts/user/query/"
        userinfo = self._getResult(uri, callname=callname, unflatJson=unflatJson)
        return userinfo

    def _CRICSiteQuery(self, callname):
        """
        :param callname: name of the call
        :return: dict of the result
        """

        uri = "/api/cms/site/query/"
        extraArgs = {"rcsite_state": "ANY"}
        sitenames = self._getResult(uri, callname=callname, args=extraArgs)
        return sitenames

    def whoAmI(self):
        """
        _whoAmI_

        Given the authentication mechanism used for this request (x509 so far),
        return information about myself, like DN/ roles/groups, etc
        :return: a list of dictionary
        """
        return self._CRICUserQuery('whoami', unflatJson=False)['result']

    def userNameDn(self, username):
        """
        _userNameDn_

        Convert CERN Nice username to DN.
        :param username: string with the username
        :return: a string wit the user's DN
        """
        ### TODO: use a different cache file and try again if the user is still not there
        userdn = ""
        userinfo = self._CRICUserQuery('people')
        for x in userinfo:
            if x['username'] == username:
                userdn = x['dn']
                break
        return userdn

    def getAllPSNs(self):
        """
        _getAllPSNs_

        Retrieve all PSNs (aka CMSNames) from CRIC
        :return: a flat list of CMS site names
        """

        sitenames = self._CRICSiteQuery(callname='site-names')
        cmsnames = [x['alias'] for x in sitenames if x['type'] == 'psn']
        return cmsnames

    def getAllPhEDExNodeNames(self, pattern=None, excludeBuffer=False):
        """
        _getAllPhEDExNodeNames_
        Retrieve all PNNs from CRIC and filter them out if a pattern has been
        provided.
        :param pattern: a regex to be applied to filter the output
        :param excludeBuffer: flag to exclude T1 Buffer endpoints
        :return: a flat list of PNNs
        """
        sitenames = self._CRICSiteQuery(callname='site-names')

        nodeNames = [x['alias'] for x in sitenames if x['type'] == 'phedex']
        if excludeBuffer:
            nodeNames = [x for x in nodeNames if not x.endswith("_Buffer")]
        if pattern and isinstance(pattern, (str, bytes)):
            pattern = re.compile(pattern)
            nodeNames = [x for x in nodeNames if pattern.match(x)]
        return nodeNames

    def PNNstoPSNs(self, pnns):
        """
        Given a list of PNNs, return all their PSNs

        :param pnns: a string or a list of PNNs
        :return: a list with unique PSNs matching those PNNs
        """
        mapping = self._CRICSiteQuery(callname='data-processing')

        if isinstance(pnns, (str, bytes)):
            pnns = [pnns]

        psns = set()
        for pnn in pnns:
            psnSet = set()
            for item in mapping:
                if pnn == item['phedex_name']:
                    psnSet.add(item['psn_name'])
            if psnSet:
                psns.update(psnSet)
            else:
                self["logger"].debug("No PSNs for PNN: %s" % pnn)
        return list(psns)

    def PSNstoPNNs(self, psns, allowPNNLess=False):
        """
        Given a list of PSNs, return all their PNNs

        :param psns: a string or a list of PSNs
        :param allowPNNLess: flag to return the PSN as a PNN if no match
        :return: a list with unique PNNs matching those PSNs
        """
        mapping = self._CRICSiteQuery(callname='data-processing')

        if isinstance(psns, (str, bytes)):
            psns = [psns]

        pnns = set()
        for psn in psns:
            pnnSet = set()
            for item in mapping:
                if item['psn_name'] == psn:
                    pnnSet.add(item['phedex_name'])
            if pnnSet:
                pnns.update(pnnSet)
            elif allowPNNLess:
                pnns.add(psn)
                self["logger"].debug("PSN %s has no PNNs. PNNLess flag enabled though.", psn)
            else:
                self["logger"].debug("No PNNs for PSN: %s" % psn)
        return list(pnns)

    def PSNtoPNNMap(self, psnPattern=''):
        """
        Given a PSN regex pattern, return a map of PSN to PNNs
        :param psnPattern: a pattern string
        :return: a dictionary key'ed by PSN names, with sets of PNNs as values
        """
        if not isinstance(psnPattern, (str, bytes)):
            raise TypeError('psnPattern argument must be of type str or bytes')

        results = self._CRICSiteQuery(callname='data-processing')
        mapping = {}

        psnPattern = re.compile(psnPattern)
        for entry in results:
            if psnPattern.match(entry['psn_name']):
                mapping.setdefault(entry['psn_name'], set()).add(entry['phedex_name'])
        return mapping

    def PNNtoPSNMap(self, pnnPattern=''):
        """
        Given a PNN regex pattern, return a map of PNN to PSNs
        :param pnnPattern: a pattern string
        :return: a dictionary key'ed by PNN names, with sets of PSNs as values
        """
        if not isinstance(pnnPattern, (str, bytes)):
            raise TypeError('pnnPattern argument must be of type str or bytes')

        results = self._CRICSiteQuery(callname='data-processing')
        mapping = {}

        pnnPattern = re.compile(pnnPattern)
        for entry in results:
            if pnnPattern.match(entry['phedex_name']):
                mapping.setdefault(entry['phedex_name'], set()).add(entry['psn_name'])
        return mapping
