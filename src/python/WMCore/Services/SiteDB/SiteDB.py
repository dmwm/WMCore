#!/usr/bin/env python
"""
_SiteDB_

API for dealing with retrieving information from SiteDB

"""

__revision__ = "$Id: SiteDB.py,v 1.5 2009/03/31 14:53:45 metson Exp $"
__version__ = "$Revision: 1.5 $"

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
        
        if os.getenv('HOME'):
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
        params = urllib.urlencode(args)
        query = callname + '?' + params
        result = ''
        if clearCache:
            self.clearCache(file)
        try:
            f = self.refreshCache(file, query)
            result = f.read()
            f.close()
        except IOError:
            raise RuntimeError("URL not available: %s" % callname )
        # When SiteDB sends proper json, we can use simplejson
        #return json.loads(result)
        return self.parser.dictParser(result)


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
