#!/usr/bin/env python
"""
_SiteDB_

API for dealing with retrieving information from SiteDB

"""

__revision__ = "$Id: SiteDB.py,v 1.11 2009/08/07 18:31:12 ewv Exp $"
__version__ = "$Revision: 1.11 $"

from WMCore.Services.Service import Service
import urllib
import logging
import os
import pwd
# This should be deprecated in preference to simplejson once SiteDB spits out
# correct json
from WMCore.Services.JSONParser.JSONParser import JSONParser
#try:
    # Python 2.6
    #import json
#except:
    # Prior to 2.6 requires simplejson
    #import simplejson as json

class SiteDBJSON(Service):

    """
    API for dealing with retrieving information from SiteDB
    """

    def __init__(self, dict={}):
        dict['endpoint'] = "https://cmsweb.cern.ch/sitedb/json/index/"
        self.parser = JSONParser()

        if os.getenv('CMS_SITEDB_CACHE_DIR'):
            dict['cachepath'] = os.getenv('CMS_SITEDB_CACHE_DIR') + '/.cms_sitedbcache'
        elif os.getenv('HOME'):
            dict['cachepath'] = os.getenv('HOME') + '/.cms_sitedbcache'
        else:
            dict['cachepath'] = '/tmp/sitedbjson_' + pwd.getpwuid(os.getuid())[0]
        if not os.path.isdir(dict['cachepath']):
            os.mkdir(dict['cachepath'])
        if 'logger' not in dict.keys():
            logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=dict['cachepath'] + '/sitedbjsonparser.log',
                    filemode='w')
            dict['logger'] = logging.getLogger('SiteDBParser')
        Service.__init__(self, dict)


    def getJSON(self, callname, file='result.json', clearCache=False, **args):
        """
        _getJSON_

        retrieve JSON formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        if clearCache:
            self.clearCache(file, args)
        try:
            f = self.refreshCache(file, callname, args)
            result = f.read()
            f.close()
        except IOError:
            raise RuntimeError("URL not available: %s" % callname )
        try:
            # When SiteDB sends proper json, we can use simplejson
            # return json.loads(result)
            results = self.parser.dictParser(result)
            return results
        except SyntaxError:
            self.clearCache(file, args)
            raise SyntaxError("Problem parsing data. Cachefile cleared. Retrying may work")


    def dnUserName(self, dn):
        """
        Convert DN to Hypernews name. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        file = 'dnUserName_%s.json' % str(dn.__hash__())
        try:
            userinfo = self.getJSON("dnUserName", dn=dn, file=file)
            userName = userinfo['user']
        except (KeyError, IndexError):
            userinfo = self.getJSON("dnUserName", dn=dn,
                                           file=file, clearCache=True)
            userName = userinfo['user']
        return userName


    def cmsNametoCE(self, cmsName):
        """
        Convert CMS name to list of CEs
        """
        file = 'cmsNametoCE_%s.json' % cmsName
        ceList = self.cmsNametoList(cmsName, 'CE', file=file)
        return ceList


    def cmsNametoSE(self, cmsName):
        """
        Convert CMS name to list of SEs
        """
        file = 'cmsNametoSE_%s.json' % cmsName
        seList = self.cmsNametoList(cmsName, 'SE', file=file)
        return seList


    def cmsNametoList(self, cmsName, kind, file):
        """
        Convert CMS name to list of CEs or SEs
        """

        cmsName = cmsName.replace('*','%')
        cmsName = cmsName.replace('?','_')
        theInfo = self.getJSON("CMSNameto"+kind, file=file, name=cmsName)

        theList = []
        for index in theInfo:
            try:
                item = theInfo[index]['name']
                if item:
                    theList.append(item)
            except KeyError:
                pass

        return theList


    def seToCMSName(self, se):
        """
        Convert SE name to the CMS Site they belong to
        """
        # Can't understand why this needs to be a list (T1/T2 sharing SE?)
        file = 'seToCMSName_%s.json' % se
        try:
            info = self.getJSON("SEtoCMSName", name=se, file=file)
            cmsName = info['0']['name']
        except (KeyError, IndexError):
            info = self.getJSON("SEtoCMSName", name=se, file=file, clearCache=True)
            cmsName = info['0']['name']
        return cmsName


    def cmsNametoPhEDExNode(self, cmsName):
        """
        Convert CMS name to list of Phedex Nodes
        """
        file = 'cmsNametoPhEDExNode_%s.json' % cmsName
        theInfo = self.getJSON("CMSNametoPhEDExNode", file=file, cms_name=cmsName)

        theList = []
        for index in theInfo:
            try:
                item = theInfo[index]['phedex_node']
                if item:
                    theList.append(item)
            except KeyError:
                pass

        return theList
