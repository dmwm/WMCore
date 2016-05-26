#!/usr/bin/env python
"""
_SiteDB_

API for dealing with interpreting information from SiteDB

"""
from WMCore.Services.SiteDB.SiteDBAPI import SiteDBAPI
from WMCore.Services.EmulatorSwitch import emulatorHook

import re

#TODO remove this when all DBS origin_site_name is converted to PNN
pnn_regex = re.compile(r'^T[0-3%]((_[A-Z]{2}(_[A-Za-z0-9]+)*)?)')

# emulator hook is used to swap the class instance
# when emulator values are set.
# Look WMCore.Services.EmulatorSwitch module for the values
@emulatorHook
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
            sitenames = filter(lambda x: x[u'site_name'] == sitename, sitenames)
        return sitenames

    def _siteresources(self, clearCache=False):
        filename = 'site-resources.json'
        return self.getJSON('site-resources', filename=filename)

    def _dataProcessing(self, pnn=None, psn=None, clearCache=False):
        """
        Returns a mapping between PNNs and PSNs.
        In case a PSN is provided, then it returns only the PNN(s) it maps to.
        In case a PNN is provided, then it returns only the PSN(s) it maps to.
        """
        filename = 'data-processing.json'
        mapping = self.getJSON('data-processing', filename=filename, clearCache=clearCache)
        if pnn:
            mapping = [item['psn_name'] for item in mapping if item['phedex_name']==pnn]
        elif psn:
            mapping = [item['phedex_name'] for item in mapping if item['psn_name']==psn]
        return mapping

    def dnUserName(self, dn):
        """
        Convert DN to Hypernews name. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        try:
            userinfo = filter(lambda x: x['dn']==dn, self._people())[0]
            username = userinfo['username']
        except (KeyError, IndexError):
            userinfo = filter(lambda x: x['dn']==dn, self._people(clearCache=True))[0]
            username = userinfo['username']
        return username

    def userNameDn(self, username):
        """
        Convert Hypernews name to DN. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        try:
            userinfo = filter(lambda x: x['username']==username, self._people())[0]
            userdn = userinfo['dn']
        except (KeyError, IndexError):
            userinfo = filter(lambda x: x['username']==username, self._people(clearCache=True))[0]
            userdn = userinfo['dn']
        return userdn

    def cmsNametoCE(self, cmsName):
        """
        Convert CMS name (also pattern) to list of CEs
        """
        raise NotImplementedError
        #return self.cmsNametoList(cmsName, 'CE')

    def cmsNametoSE(self, cmsName):
        """
        Convert CMS name (also pattern) to list of SEs
        """
        return self.cmsNametoList(cmsName, 'SE')

    def getAllCENames(self):
        """
        _getAllCENames_

        Get all CE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        siteresources = self._siteresources()
        ceList = filter(lambda x: x['type']=='CE', siteresources)
        ceList = map(lambda x: x['fqdn'], ceList)
        return ceList

    def getAllSENames(self):
        """
        _getAllSENames_

        Get all SE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        siteresources = self._siteresources()
        seList = filter(lambda x: x['type']=='SE', siteresources)
        seList = map(lambda x: x['fqdn'], seList)
        return seList

    def getAllCMSNames(self):
        """
        _getAllCMSNames_

        Get all the CMSNames from siteDB
        This will allow us to add them in resourceControl at once
        """
        sitenames = self._sitenames()
        cmsnames = filter(lambda x: x['type']=='psn', sitenames)
        cmsnames = map(lambda x: x['alias'], cmsnames)
        return cmsnames
    
    def getAllPhEDExNodeNames(self, excludeBuffer = False):
        """
        _getAllPhEDExNodeNames_

        Get all the CMSNames from siteDB
        This will allow us to add them in resourceControl at once
        """
        sitenames = self._sitenames()
        node_names = filter(lambda x: x['type']=='phedex', sitenames)
        node_names = map(lambda x: x['alias'], node_names)
        if excludeBuffer:
            node_names = filter(lambda x: not x.endswith("_Buffer"), node_names)
        return node_names

    def cmsNametoList(self, cmsname_pattern, kind):
        """
        Convert CMS name pattern T1*, T2* to a list of CEs or SEs.
        """
        cmsname_pattern = cmsname_pattern.replace('*','.*')
        cmsname_pattern = cmsname_pattern.replace('%','.*')
        cmsname_pattern = re.compile(cmsname_pattern)

        sitenames = filter(lambda x: x[u'type']=='psn' and cmsname_pattern.match(x[u'alias']),
                           self._sitenames())
        sitenames = set(map(lambda x: x['site_name'], sitenames))
        siteresources = filter(lambda x: x['site_name'] in sitenames, self._siteresources())
        hostlist = filter(lambda x: x['type']==kind, siteresources)
        hostlist = map(lambda x: x['fqdn'], hostlist)

        return hostlist

    def ceToCMSName(self, ce):
        """
        Convert SE name to the CMS Site they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of cms site alias
        """
        try:
            siteresources = filter(lambda x: x['fqdn']==ce, self._siteresources())
        except IndexError:
            return None
        siteNames = []
        for resource in siteresources:
            siteNames.extend(self._sitenames(sitename=resource['site_name']))
        cmsname = filter(lambda x: x['type']=='cms', siteNames)
        return [x['alias'] for x in cmsname]

    def seToCMSName(self, se):
        """
        Convert SE name to the CMS Site they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of cms site alias
        """
        try:
            siteresources = filter(lambda x: x['fqdn']==se, self._siteresources())
        except IndexError:
            return None
        siteNames = []
        for resource in siteresources:
            siteNames.extend(self._sitenames(sitename=resource['site_name']))
        cmsname = filter(lambda x: x['type']=='cms', siteNames)
        return [x['alias'] for x in cmsname]

    def seToPNNs(self, se):
        """
        Convert SE name to the PNN they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of pnns
        """
        try:
            siteresources = filter(lambda x: x['fqdn']==se, self._siteresources())
        except IndexError:
            return None
        siteNames = []
        for resource in siteresources:
            siteNames.extend(self._sitenames(sitename=resource['site_name']))
        pnns = filter(lambda x: x['type']=='phedex', siteNames)
        return [x['alias'] for x in pnns]


    def cmsNametoPhEDExNode(self, cmsName):
        """
        Convert CMS name to list of Phedex Nodes
        """
        sitenames = self._sitenames()
        try:
            sitename = filter(lambda x: x['type']=='cms' and x['alias']==cmsName, sitenames)[0]['site_name']
        except IndexError:
            return None
        phedexnames = filter(lambda x: x['type']=='phedex' and x['site_name']==sitename, sitenames)
        phedexnames = map(lambda x: x['alias'], phedexnames)
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
            if pnn=="T0_CH_CERN_Export" or pnn.endswith("_MSS") or pnn.endswith("_Buffer"):
                continue
            psn_list = self.PNNtoPSN(pnn)
            psns.update(psn_list)
            if not psn_list:
                self["logger"].warning("No PSNs for PNN: %s" % pnn)
        return list(psns)

    def PSNstoPNNs(self, psns):
        """
        Convert list of Processing Site Names to PhEDEx Node Names
        """
        pnns = set()
        for psn in psns:
            pnn_list = self.PSNtoPNN(psn)
            if not pnn_list:
                self["logger"].warning("No PNNs for PSN: %s" % psn)
            pnns.update(pnn_list)
        return list(pnns)

    def PSNtoPNNMap(self, psn_pattern=''):
        if not isinstance(psn_pattern, str):
            raise TypeError('psn_pattern arg must be of type str')

        mapping = {}
        psn_pattern = re.compile(psn_pattern)  # .replace('*', '.*').replace('%', '.*'))
        for entry in self._dataProcessing():
            if not psn_pattern.match(entry['psn_name']):
                continue
            mapping.setdefault(entry['psn_name'], set()).add(entry['phedex_name'])
        return mapping
    
    #TODO remove this when all DBS origin_site_name is converted to PNN
    def checkAndConvertSENameToPNN(self, seNameOrPNN):
        """
        check whether argument is sename 
        if it is convert to PNN
        if not just return argument
        """
        if isinstance(seNameOrPNN, basestring):
            seNameOrPNN = [seNameOrPNN]
        
        newList = []
        for se in seNameOrPNN:
            if not pnn_regex.match(se):
                newList.extend(self.seToPNNs(se))
            else:
                newList.append(se)
        return newList
