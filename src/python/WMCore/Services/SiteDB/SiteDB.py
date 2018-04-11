#!/usr/bin/env python
"""
_SiteDB_

API for dealing with interpreting information from SiteDB

"""
from WMCore.Services.SiteDB.SiteDBAPI import SiteDBAPI

import re


class SiteDBJSON(SiteDBAPI):

    """
    API for dealing with interpreting information from SiteDB
    """

    def _people(self, username=None, clearCache=False):
        if username:
            filename = 'people_%s.json' % (username)
            people = self.getJSON("people", filename=filename, clearCache=clearCache, data=dict(match=username))
        else:
            filename = 'people.json'
            people = self.getJSON("people", filename=filename, clearCache=clearCache)
        return people

    def _sitenames(self, sitename=None, clearCache=False):
        filename = 'site-names.json'
        sitenames = self.getJSON('site-names', filename=filename, clearCache=clearCache)
        if sitename:
            sitenames = [x for x in sitenames if x[u'site_name'] == sitename]
        return sitenames

    def _siteresources(self, clearCache=False):
        filename = 'site-resources.json'
        return self.getJSON('site-resources', filename=filename, clearCache=clearCache)

    def _dataProcessing(self, pnn=None, psn=None, clearCache=False):
        """
        Returns a mapping between PNNs and PSNs.
        In case a PSN is provided, then it returns only the PNN(s) it maps to.
        In case a PNN is provided, then it returns only the PSN(s) it maps to.
        """
        filename = 'data-processing.json'
        mapping = self.getJSON('data-processing', filename=filename, clearCache=clearCache)
        if pnn:
            mapping = [item['psn_name'] for item in mapping if item['phedex_name'] == pnn]
        elif psn:
            mapping = [item['phedex_name'] for item in mapping if item['psn_name'] == psn]
        return mapping

    def dnUserName(self, dn):
        """
        Convert DN to Hypernews name. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        try:
            userinfo = [x for x in self._people() if x['dn'] == dn][0]
            username = userinfo['username']
        except (KeyError, IndexError):
            userinfo = [x for x in self._people(clearCache=True) if x['dn'] == dn][0]
            username = userinfo['username']
        return username

    def userNameDn(self, username):
        """
        Convert Hypernews name to DN. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        try:
            userinfo = [x for x in self._people() if x['username'] == username][0]
            userdn = userinfo['dn']
        except (KeyError, IndexError):
            userinfo = [x for x in self._people(clearCache=True) if x['username'] == username][0]
            userdn = userinfo['dn']
        return userdn

    def cmsNametoCE(self, cmsName):
        """
        Convert CMS name (also pattern) to list of CEs
        """
        raise NotImplementedError

    def getAllCENames(self):
        """
        _getAllCENames_

        Get all CE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        siteresources = self._siteresources()
        ceList = [x['fqdn'] for x in siteresources if x['type'] == 'CE']
        return ceList

    def getAllSENames(self):
        """
        _getAllSENames_

        Get all SE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        siteresources = self._siteresources()
        seList = [x['fqdn'] for x in siteresources if x['type'] == 'SE']
        return seList

    def getAllCMSNames(self):
        """
        _getAllCMSNames_

        Get all the CMSNames from siteDB
        This will allow us to add them in resourceControl at once
        """
        sitenames = self._sitenames()
        cmsnames = [x['alias'] for x in sitenames if x['type'] == 'psn']
        return cmsnames

    def getAllPhEDExNodeNames(self, pattern=None, excludeBuffer=False):
        """
        _getAllPhEDExNodeNames_

        Get all the CMSNames from siteDB
        This will allow us to add them in resourceControl at once
        """
        sitenames = self._sitenames()
        nodeNames = [x['alias'] for x in sitenames if x['type'] == 'phedex']
        if excludeBuffer:
            nodeNames = [x for x in nodeNames if not x.endswith("_Buffer")]
        if pattern and isinstance(pattern, basestring):
            pattern = re.compile(pattern)
            nodeNames = [x for x in nodeNames if pattern.match(x)]

        return nodeNames

    def cmsNametoList(self, cmsnamePattern, kind):
        """
        Convert CMS name pattern T1*, T2* to a list of CEs or SEs.
        """
        cmsnamePattern = cmsnamePattern.replace('*', '.*')
        cmsnamePattern = cmsnamePattern.replace('%', '.*')
        cmsnamePattern = re.compile(cmsnamePattern)

        sitenames = set([x['site_name'] for x in self._sitenames() if x[u'type'] == 'psn'
                         and cmsnamePattern.match(x[u'alias'])])
        siteresources = [x for x in self._siteresources() if x['site_name'] in sitenames]
        hostlist = [x['fqdn'] for x in siteresources if x['type'] == kind]

        return hostlist

    def ceToCMSName(self, ce):
        """
        Convert SE name to the CMS Site they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of cms site alias
        """
        try:
            siteresources = [x for x in self._siteresources() if x['fqdn'] == ce]
        except IndexError:
            return None
        siteNames = []
        for resource in siteresources:
            siteNames.extend(self._sitenames(sitename=resource['site_name']))
        cmsname = [x for x in siteNames if x['type'] == 'cms']
        return [x['alias'] for x in cmsname]

    def cmsNametoPhEDExNode(self, cmsName):
        """
        Convert CMS name to list of Phedex Nodes
        """
        sitenames = self._sitenames()
        try:
            sitename = [x for x in sitenames if x['type'] == 'cms' and x['alias'] == cmsName][0]['site_name']
        except IndexError:
            return None
        phedexnames = [x['alias'] for x in sitenames if x['type'] == 'phedex' and x['site_name'] == sitename]
        return phedexnames


    def PNNtoPSN(self, pnn):
        """
        Convert PhEDEx node name to Processing Site Name(s)
        """
        return self._dataProcessing(pnn=pnn)

    def PSNtoPNN(self, psn):
        """
        Convert Processing Site Name to PhEDEx Node Name(s)
        """
        return self._dataProcessing(psn=psn)

    def PNNstoPSNs(self, pnns):
        """
        Convert list of PhEDEx node names to Processing Site Name(s)
        """
        psns = set()
        for pnn in pnns:
            if pnn == "T0_CH_CERN_Export" or pnn.endswith("_MSS") or pnn.endswith("_Buffer"):
                continue
            psnList = self.PNNtoPSN(pnn)
            psns.update(psnList)
            if not psnList:
                self["logger"].warning("No PSNs for PNN: %s" % pnn)
        return list(psns)

    def PSNstoPNNs(self, psns):
        """
        Convert list of Processing Site Names to PhEDEx Node Names
        """
        pnns = set()
        for psn in psns:
            pnnList = self.PSNtoPNN(psn)
            if not pnnList:
                self["logger"].warning("No PNNs for PSN: %s" % psn)
            pnns.update(pnnList)
        return list(pnns)

    def PSNtoPNNMap(self, psnPattern=''):
        if not isinstance(psnPattern, str):
            raise TypeError('psn_pattern arg must be of type str')

        mapping = {}
        psnPattern = re.compile(psnPattern)  # .replace('*', '.*').replace('%', '.*'))
        for entry in self._dataProcessing():
            if not psnPattern.match(entry['psn_name']):
                continue
            mapping.setdefault(entry['psn_name'], set()).add(entry['phedex_name'])
        return mapping
