#!/usr/bin/env python
"""
_SiteDB_

API for dealing with retrieving information from SiteDB

"""
from WMCore.Services.Service import Service
from WMCore.Services.EmulatorSwitch import emulatorHook

import json
import re

def row2dict(columns, row):
    """Convert rows to dictionaries with column keys from description"""
    robj = {}
    for k,v in zip(columns, row):
        robj.setdefault(k,v)
    return robj

def unflattenJSON(data):
    """Tranform input to unflatten JSON format"""
    columns = data['desc']['columns']
    return [row2dict(columns, row) for row in data['result']]

# emulator hook is used to swap the class instance
# when emulator values are set.
# Look WMCore.Services.EmulatorSwitch module for the values
@emulatorHook
class SiteDBJSON(Service):

    """
    API for dealing with retrieving information from SiteDB
    """
    def __init__(self, config={}):
        config = dict(config)
        config['endpoint'] = "https://cmsweb.cern.ch/sitedb/data/prod/"
        Service.__init__(self, config)

    def getJSON(self, callname, file = 'result.json', clearCache = False, verb = 'GET', data={}):
        """
        _getJSON_

        retrieve JSON formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        if clearCache:
            self.clearCache(cachefile=file, inputdata=data, verb = verb)
        try:
            #Set content_type and accept_type to application/json to get json returned from siteDB.
            #Default is text/html which will return xml instead
            #Add accept-encoding to gzip,identity to overwrite httplib default gzip,deflate,
            #which is not working properly with cmsweb
            f = self.refreshCache(cachefile=file, url=callname, inputdata=data,
                                  verb = verb, contentType='application/json',
                                  incoming_headers={'Accept' : 'application/json',
                                                    'accept-encoding' : 'gzip,identity'})
            result = f.read()
            f.close()
        except IOError:
            raise RuntimeError("URL not available: %s" % callname )
        try:
            results = json.loads(result)
            results = unflattenJSON(results)
            return results
        except SyntaxError:
            self.clearCache(file, args, verb = verb)
            raise SyntaxError("Problem parsing data. Cachefile cleared. Retrying may work")

    def _people(self, username=None, clearCache=False):
        if username:
            file = 'people_%s.json' % (username)
            people = self.getJSON("people", file=file, clearCache=clearCache, data=dict(match=username))
        else:
            file = 'people.json'
            people = self.getJSON("people", file=file, clearCache=clearCache)
        return people

    def _sitenames(self, sitename=None, clearCache=False):
        file = 'site-names.json'
        sitenames = self.getJSON('site-names', file=file, clearCache=clearCache)
        if sitename:
            sitenames = filter(lambda x: x[u'site_name'] == sitename, sitenames)
        return sitenames

    def _siteresources(self, clearCache=False):
        file = 'site-resources.json'
        return self.getJSON('site-resources', file=file)

    def _dataProcessing(self, pnn=None, clearCache=False):
        file = 'data-processing.json'
        psnMap = self.getJSON('data-processing', file=file, clearCache=clearCache)
        if pnn:
            psnMap = filter(lambda x: x['phedex_name'] == pnn, psnMap)
        return psnMap

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

    def cmsNametoList(self, cmsname_pattern, kind, file=None):
        """
        Convert CMS name pattern T1*, T2* to a list of CEs or SEs. The file is
        for backward compatibility with SiteDBv1
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

        psnMap = self._dataProcessing()
        try:
            reducedMap = filter(lambda x: x[u'phedex_name']==pnn, psnMap)
            psns = [x[u'psn_name'] for x in reducedMap]
        except IndexError:
            return None
        return psns
