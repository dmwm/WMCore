#!/usr/bin/env python
"""
_SiteDB_

API for dealing with retrieving information from SiteDB

"""
from WMCore.Services.Service import Service
from WMCore.Services.EmulatorSwitch import emulatorHook

import json

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
        if sitename:
            file = 'site-names_%s.json' % (sitename)
            sitenames = self.getJSON('site-names', file=file, clearCache=clearCache, data=dict(match=sitename))
        else:
            file = 'site-names.json'
            sitenames = self.getJSON('site-names', file=file, clearCache=clearCache)
        return sitenames

    def _siteresources(self, clearCache=False):
        file = 'site-resources.json'
        return self.getJSON('site-resources', file=file)

    def dnUserName(self, dn):
        """
        Convert DN to Hypernews name. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        try:
            userinfo = filter(lambda x: x['dn']==dn, self._people())[0]
            username = userinfo['username']
        except (KeyError, IndexError):
            userinfo = filter(lambda x: x['dn']==dn, self._people())[0]
            username = userinfo['username']
        return username

    def cmsNametoCE(self, cmsName):
        """
        Convert CMS name to list of CEs
        """
        sitenames = filter(lambda x: x['type']=='cms' and x['alias']==cmsName, self._sitenames())[0]
        siteresources = filter(lambda x: x['site_name']==sitenames['site_name'], self._siteresources())
        ceList = filter(lambda x: x['type']=='CE', siteresources)
        ceList = map(lambda x: x['fqdn'], ceList)
        return ceList

    def cmsNametoSE(self, cmsName):
        """
        Convert CMS name to list of SEs
        """
        sitenames = filter(lambda x: x['type']=='cms' and x['alias']==cmsName, self._sitenames())[0]
        siteresources = filter(lambda x: x['site_name']==sitenames['site_name'], self._siteresources())
        seList = filter(lambda x: x['type']=='SE', siteresources)
        seList = map(lambda x: x['fqdn'], seList)
        return seList

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
        cmsnames = filter(lambda x: x['type']=='cms', sitenames)
        cmsnames = map(lambda x: x['alias'], cmsnames)
        return cmsnames

    def cmsNametoList(self, cmsName, kind, file):
        if kind=='CE':
            return cmsNametoCE(cmsName)
        elif kind=='SE':
            return cmsNametoSE(cmsName)
        else:
            raise NotImplemented('cmsNametoList for kind: %s is not yet implemented' % (kind))

    def seToCMSName(self, se):
        """
        Convert SE name to the CMS Site they belong to
        """
        siteresources = filter(lambda x: x['fqdn']==se, self._siteresources())[0]
        cmsname = filter(lambda x: x['type']=='cms', self._sitenames(sitename=siteresources['site_name']))[0]['alias']
        return cmsname

    def cmsNametoPhEDExNode(self, cmsName):
        """
        Convert CMS name to list of Phedex Nodes
        """
        sitenames = self._sitenames()
        sitename = filter(lambda x: x['type']=='cms' and x['alias']==cmsName, sitenames)[0]['site_name']
        phedexnames = filter(lambda x: x['type']=='phedex' and x['site_name']==sitename, sitenames)
        phedexnames = map(lambda x: x['alias'], phedexnames)
        return phedexnames


    def phEDExNodetocmsName(self, node):
        """
        Convert PhEDEx node name to cms site
        """
        # api doesn't work at the moment - so reverse engineer
        # first strip special endings and check with cmsNametoPhEDExNode
        # if this fails (to my knowledge no node does fail) do a full lookup
        name = node.replace('_MSS',
                            '').replace('_Buffer',
                                        '').replace('_Export', '')

        return name
        # Disable cross-check until following bug fixed.
        # https://savannah.cern.ch/bugs/index.php?67044
#        if node in self.cmsNametoPhEDExNode(name):
#            return name
#
#        # As far as i can tell there is no way to get a full listing, would
#        # need to call CMSNametoPhEDExNode?cms_name= but can't find a way to do
#        # that. So simply raise an error
#        raise ValueError, "Unable to find CMS name for \'%s\'" % node
