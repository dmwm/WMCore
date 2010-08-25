#!/usr/bin/env python
"""
_SiteDB_

API for dealing with retrieving information from SiteDB

"""

__revision__ = "$Id: SiteDB.py,v 1.4 2009/03/25 14:39:36 ewv Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Services.JSONParser import JSONParser
import urllib

class SiteDBJSON:

    """
    API for dealing with retrieving information from SiteDB
    """

    def __init__(self):
        siteDBURL = "https://cmsweb.cern.ch/sitedb/json/index/"
        self.parser = JSONParser.JSONParser(siteDBURL)


    def dnUserName(self, dn):
        """
        Convert DN to Hypernews name. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        file = 'dnUserName_%s.json' % str(dn.__hash__())
        try:
            userinfo = self.parser.getJSON("dnUserName", dn=dn, file=file)
            userName = userinfo['user']
        except (KeyError, IndexError):
            userinfo = self.parser.getJSON("dnUserName", dn=dn,
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
        theInfo = self.parser.getJSON("CMSNameto"+kind, file=file, name=cmsName)

        theList = []
        for index in theInfo:
            try:
                item = theInfo[index]['name']
                if item:
                    theList.append(item)
            except KeyError:
                pass

        return theList
